"""
signal_executor_controller.py
Hummingbot Strategies V2 controller that:
1. Subscribes to MQTT topic for trading signals
2. Spawns one PositionExecutor per trading-pair with:
   • 5 take-profit levels (20 % each)
   • trailing stop-loss
   • hard time-out
3. Ignores duplicate signals for the SAME pair while the executor is still running.
4. Cleans the registry once the executor closes.

Prerequisites
-------------
MQTT bridge enabled in Hummingbot configuration
"""

import asyncio
import hashlib
import json
import time
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import Field

from hummingbot.client.config.config_helpers import load_client_config_map_from_file
from hummingbot.core.data_type.common import MarketDict, OrderType, PositionSide, PriceType, TradeType
from hummingbot.remote_iface.mqtt import ExternalEventFactory, ExternalTopicFactory
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction , StopExecutorAction


class SignalExecutorConfig(ControllerConfigBase):
    controller_type: str = "signal_executor"
    controller_name: str = "signal_executor_controller"
    connector_name: str = Field(
        default="binance",
        json_schema_extra={
            "prompt": "Enter the connector name (e.g., binance_perpetual): ",
            "prompt_on_new": True}
    )
    trading_pair: str = Field(
        default="ETH-USDT, BTC-USDT",
        json_schema_extra={
            "prompt": "Enter the trading pair to trade on (e.g., WLD-USDT): ",
            "prompt_on_new": True}
    )
    max_open_trades: int = Field(
        default=3,
        json_schema_extra={
            "prompt": "Maximum number of concurrent open trades: ",
            "prompt_on_new": True}
    )
    position_size_usd: Decimal = Field(
        default=Decimal("10"),
        json_schema_extra={
            "prompt": "Position size in USD: ",
            "prompt_on_new": True}
    )
    mqtt_topic_level: str = Field(
        default="signal_executor/execute_trading_signal",
        json_schema_extra={
            "prompt": "Enter the MQTT topic to subscribe to: ",
            "prompt_on_new": True}
    )
    heartbeat_interval: int = Field(
        default=100,    # 100 iterations default      
        json_schema_extra={
            "prompt": "Heartbeat log interval (iterations): ",
            "prompt_on_new": True}
    )
    heartbeat_time_interval: int = Field(
        default=60, # 60 seconds default
        json_schema_extra={
            "prompt": "Heartbeat log interval (seconds): ",
            "prompt_on_new": True}
    )
    time_limit_seconds: int = Field(
        default=172800, # 2 days default
        json_schema_extra={
            "prompt": "Position time limit in seconds (default 172800 = 2 days): ",
            "prompt_on_new": True}
    )
    partial_take_profit_ratio: Decimal = Field(
        default=Decimal("1"),
        json_schema_extra={
            "prompt": "Partial take profit ratio (1.0 = 100%, 0.5 = 50%): ",
            "prompt_on_new": True}
    )
    
    position_max_initial_idle_time: int = Field(
        default=14400,  # 4 hours default
        json_schema_extra={
            "prompt": "Max seconds allowed before cancelling a position that never traded: ",
            "prompt_on_new": True}
    )

    def update_markets(self, markets: MarketDict) -> MarketDict:
        # This is required for controllers
        print("Updating markets for SignalExecutorController...")
        print(f"DEBUG: Initial trading pairs: {self.trading_pair}")
        trading_pairs = [pair.strip() for pair in self.trading_pair.split(',')]
        for pair in trading_pairs:
            markets.add_or_update(self.connector_name, pair)
        print(f"DEBUG: Final markets content: {markets}")
        return markets


class SignalExecutorInternalConfig:
    def __init__(
        self,
        reference_payload: dict = None,
        max_payload_size_factor: int = 4
    ):
    """
    Internal configuration for payload size validation.
    :param reference_payload: A sample payload to calculate the max size.
    :param max_payload_size_factor: Factor to multiply the reference payload size.
    """
    # Default reference payload if not provided
    if reference_payload is None:
        reference_payload = {
            "trading_pair": "BTC-USDT",
            "side": "SELL",
            "buy_range": ["42000", "42500"],
            "stop_loss": "43000",
            "take_profits": ["41500", "41000", "40500", "40000", "39500"],
            "trading_time": 1800
        }
    self.reference_payload = reference_payload
    self.max_payload_size_factor = max_payload_size_factor

    @property
    def max_payload_size(self) -> int:
        import json
        ref_size = len(json.dumps(self.reference_payload).encode("utf-8"))
        return self.max_payload_size_factor * ref_size

    def is_payload_size_valid(self, payload: dict) -> bool:
        import json
        payload_size = len(json.dumps(payload).encode("utf-8"))
        return payload_size <= self.max_payload_size


class SignalExecutorController(ControllerBase):
    def __init__(self, config: SignalExecutorConfig, market_data_provider=None, actions_queue=None):
        super().__init__(config, market_data_provider, actions_queue)
        self.config: SignalExecutorConfig = config
        self._registry: Dict[str, PositionExecutorConfig] = {}
        self._market_data_provider = market_data_provider
        self._actions_queue = actions_queue
        self._paper_trade_warning_logged = False

        # Internal config for payload size validation
        self._internal_config = SignalExecutorInternalConfig()

        # MQTT consumer state variables
        self._mqtt_topic_queue = None
        self._heartbeat_counter = 0
        self._last_heartbeat_time = time.time()
        self._mqtt_initialized = False
        self._first_heartbeat_logged = False
        self._mqtt_reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._reconnect_delay = 1  # Initial delay in seconds
        self._signal_listener = None
        
    def _stop_mqtt(self):
        """Stop MQTT bridge"""
        if self._mqtt is not None:
            try:
                self._mqtt.stop()
                self._mqtt = None
                self.logger().info("MQTT Bridge disconnected")
            except Exception as e:
                self.logger().error(f'Failed to stop MQTT Bridge: {str(e)}')
        else:
            self.logger().info("MQTT Bridge is not running")

    def _notify(self, message: str):
        """Simple notification method"""
        self.logger().info(message)

    def _initialize_signal_listener(self):
        """Initialize Signal queue-based listener with retry logic"""
        topic = f"hbot/{self.config.mqtt_topic_level}"
        try:
            self._signal_listener = ExternalTopicFactory.create_async(
                topic=topic,
                callback=self._handle_received_signal,
                use_bot_prefix=False,
            )
            self.logger().info(f"Signal listener initialized successfully for topic: {topic}")

        except Exception as e:
            self.logger().error(f"Error initializing signal listener: {e}", exc_info=True)
            self._signal_listener = None
            self.logger().info("stopping controller due to signal listener initialization failure")
            self.stop()

        try:
            self.logger().info(f"Initializing Signal receiving queue for topic: {topic}")
            self._mqtt_topic_queue = ExternalTopicFactory.create_queue(topic, use_bot_prefix=False)
            self._mqtt_initialized = True
            self.logger().info("Signal receiving queue established successfully")
        except Exception as e:
            self.logger().error(f"Error initializing Signal receiving queue: {e}", exc_info=True)
            self.logger().info("stopping controller due to Signal receiving queue initialization failure")
            self.stop()

    def _handle_received_signal(self, signal: dict, topic: str):
        """Handle received signal """
        # Validate payload size before adding to queue
        if not self._internal_config.is_payload_size_valid(signal):
            self.logger().error(
                f"Received signal payload size exceeds max allowed {self._internal_config.max_payload_size} bytes. Signal ignored."
            )
            return

        self.logger().info(f"Received signal on topic {topic}: {signal}")

        if self._mqtt_topic_queue is not None:
            self.queue_add = self.queue_add + 1
            # Add signal to the queue for processing
            self.logger().info(f"queue received signal for processing")
            self._mqtt_topic_queue.append((topic, signal))
        else:
            self.logger().error("Signal receiving queue is not initialized. Cannot process signal.")

    # ------------------------------------------------------------------ #
    # Queue-based signal processing                                      #
    # ------------------------------------------------------------------ #
    def process_mqtt_messages(self):
        """Process received signals from the queue during on_tick"""
        if self._mqtt_topic_queue is not None and len(self._mqtt_topic_queue) > 0:
            self.logger().info(f"Processing {len(self._mqtt_topic_queue)} signals from the queue")
            while len(self._mqtt_topic_queue) > 0:
                entry = self._mqtt_topic_queue.popleft()
                topic, signal = entry
                self.logger().info(f"Processing received signal from topic {topic}")
                self.logger().debug(f"Signal content: {signal}")

                # Process the signal using the existing signal handling logic
                asyncio.create_task(self._handle_signal(signal))

    def stop(self) -> None:
        """
        Stop the controller. This method is called by the strategy.
        """
        self.logger().info("Stopping SignalExecutorController...")
        try:
            self._stop_mqtt_queue()
        except Exception as e:
            self.logger().warning(f"Warning during stop: {str(e)}")
        finally:
            super().stop()

    def on_stop(self):
        """
        Cleanup method that will be called when the control loop stops.
        """
        try:
            self._stop_mqtt_queue()
        except Exception as e:
            self.logger().warning(f"Warning during on_stop: {str(e)}")
        finally:
            super().on_stop()

    async def async_stop(self):
        """
        Async version of stop for explicit cleanup when needed.
        """
        try:
            self._stop_mqtt_queue()
            await self._stop_mqtt_bridge_if_needed()
        except Exception as e:
            self.logger().warning(f"Warning during async stop: {str(e)}")

    async def _stop_mqtt_bridge_if_needed(self):
        """Stop MQTT bridge if it was started by this controller"""
        try:
            # Check if MQTT is running
            if self._mqtt is not None and self._mqtt.health:
                self.logger().info("Stopping MQTT Bridge automatically...")
                self._stop_mqtt()
                self.logger().info("MQTT Bridge stopped successfully")
            else:
                self.logger().info("MQTT Bridge is not running")

        except Exception as e:
            self.logger().warning(f"Failed to stop MQTT Bridge automatically: {str(e)}")

    def _stop_mqtt_queue(self):
        """Stop MQTT queue"""
        if self._mqtt_topic_queue is not None:
            try:
                self.logger().info("Stopping MQTT queue")
                self._mqtt_topic_queue = None
                self.logger().info("MQTT queue stopped")
            except Exception as e:
                self.logger().warning(f"Warning while stopping MQTT queue: {str(e)}")
                self._mqtt_topic_queue = None

    async def update_processed_data(self):
        """
        This method is called periodically by the control loop.
        We'll use it for heartbeat logging and Signal queue status monitoring.
        """
        if self._market_data_provider.ready and self._signal_listener is None:
            # Initialize signal listener queue-based listener
            self._initialize_signal_listener()
        
        if not self._first_heartbeat_logged:
            self.logger().info(f"SignalExecutorController first heartbeat - running (iteration 0)")
            self._first_heartbeat_logged = True

        try:
            self._heartbeat_counter += 1
            current_time = time.time()

            # Log heartbeat periodically
            if (self._heartbeat_counter % self.config.heartbeat_interval == 0 or
                    current_time - self._last_heartbeat_time >= self.config.heartbeat_time_interval):
                self.logger().info(f"SignalExecutorController heartbeat - running (iteration {self._heartbeat_counter})")
                self.logger().info(f"Signal queue status: {'Connected' if self._mqtt_topic_queue is not None else 'Disconnected'}")
                self._last_heartbeat_time = current_time

            # Process MQTT messages from the queue
            self.process_mqtt_messages()
        except Exception as e:
            self.logger().error(f"Signal consumer error: {e}", exc_info=True)

    # ------------------------------------------------------------------ #
    # Helpers                                                            #
    # ------------------------------------------------------------------ #
    def _max_open_trades_reached(self) -> bool:
        """Return True if the total number of *active* executors is >= max_allowed."""
        active = [
            ex for ex in self.executors_info
            if not ex.is_done
        ]
        return len(active) >= self.config.max_open_trades

    def _pair_already_trading(self, pair: str) -> bool:
        """Return True if *any* active executor is already working on this pair."""
        return any(
            ex.trading_pair == pair
            and not ex.is_done
            for ex in self.executors_info
        )

    def _check_paper_trade_connector(self) -> bool:
        """Check if using paper trade connector and log warning once"""
        is_paper_trade = "paper_trade" in self.config.connector_name.lower()

        if is_paper_trade and not self._paper_trade_warning_logged:
            self.logger().warning(
                f"Using paper trade connector which may not have trading_rules. "
                f"This might cause issues with the position executor."
            )
            self._paper_trade_warning_logged = True

            # Check if the connector has trading_rules attribute
            connector = self._market_data_provider.get_connector(self.config.connector_name)
            if not hasattr(connector, "trading_rules"):
                self.logger().error(
                    f"The connector {self.config.connector_name} does not have trading_rules attribute. "
                    f"This will cause errors with the position executor. "
                    f"Consider using a different connector or adding trading_rules to the connector."
                )
                return False

        return True

    def _generate_signal_hash(self, signal: dict) -> str:
        """Generate a unique hash for a signal to detect duplicates"""
        sig_json = json.dumps(signal, sort_keys=True, separators=(",", ":"))
        return hashlib.md5(sig_json.encode()).hexdigest()[:8]

    def _is_duplicate_signal(self, sig_hash: str) -> bool:
        """Check if a signal is a duplicate"""
        return any(k.endswith(f"_{sig_hash}") for k in self._registry)

    # ------------------------------------------------------------------ #
    # Signal validation and processing                                   #
    # ------------------------------------------------------------------ #
    def _validate_signal(self, signal: dict) -> Tuple[bool, str]:
        """Validate the signal structure and content"""
        required_fields = ["trading_pair", "side", "buy_range", "stop_loss", "take_profits", "trading_time"]
        for field in required_fields:
            if field not in signal:
                return False, f"Missing required field: {field}"

        try:
            pair = signal["trading_pair"]
            if not pair:
                return False, "Empty trading_pair"

            side = signal["side"].upper()
            if side not in ["BUY", "SELL"]:
                return False, f"Invalid side: {side}"

            buy_low, buy_high = map(Decimal, signal["buy_range"])
            if not buy_low or not buy_high or buy_low >= buy_high:
                return False, f"Invalid buy range: {signal['buy_range']}"

            sl_price = Decimal(str(signal["stop_loss"]))
            if sl_price is None or sl_price <= 0:
                return False, f"Invalid stop loss price: {sl_price}"

            # Direction-aware stop loss validation
            if side == "BUY":
                if sl_price >= buy_low:
                    return False, f"Stop loss must be below buy range for BUY: {sl_price} >= {buy_low}"
            else:  # SELL
                if sl_price <= buy_high:
                    return False, f"Stop loss must be above buy range for SELL: {sl_price} <= {buy_high}"

            tp_prices = [Decimal(str(p)) for p in signal["take_profits"]]
            if not tp_prices or len(tp_prices) < 2:
                return False, f"Invalid take profit input: {tp_prices}"

            tp1 = tp_prices[0]
            tp2 = tp_prices[1]

            # Direction-aware take profit validation
            if side == "BUY":
                if tp1 <= buy_high or tp2 <= tp1:
                    return False, f"Invalid take profit levels for BUY: {tp_prices}"
            else:  # SELL
                if tp1 >= buy_low or tp2 >= tp1:
                    return False, f"Invalid take profit levels for SELL: {tp_prices}"

            ttl = int(signal["trading_time"])
            if not ttl or ttl <= 0:
                return False, f"Invalid trading time: {ttl}"

        except (ValueError, TypeError) as e:
            return False, f"Data type error: {str(e)}"

        return True, "Valid signal"

    def _calculate_position_parameters(self, signal: dict) -> dict:
        """Calculate position parameters from signal"""
        buy_low, buy_high = map(Decimal, signal["buy_range"])
        sl_price = Decimal(str(signal["stop_loss"]))
        ttl = int(signal["trading_time"])
        tp_prices = [Decimal(str(p)) for p in signal["take_profits"]]
        tp1 = tp_prices[0]
        tp2 = tp_prices[1]

        # Calculate percentages relative to buy_high
        tp1_percentage = (tp1 - buy_high) / buy_high
        tp2_percentage = (tp2 - buy_high) / buy_high
        
        # Calculate position size and entry price
        position_size = self.config.position_size_usd / buy_high

        mid_buy_range = (buy_low + buy_high) / Decimal("2")
        sl_percentage = abs((sl_price - mid_buy_range) / mid_buy_range)
        
        entry_price = mid_buy_range
        half_span = (buy_high - buy_low) / Decimal("2")
        activation_pct = half_span / entry_price

        return {
            "position_size": position_size,
            "entry_price": entry_price,
            "activation_pct": activation_pct,
            "tp1_percentage": tp1_percentage,
            "tp2_percentage": tp2_percentage,
            "sl_percentage": sl_percentage,
            "trading_time": ttl
        }

    # ------------------------------------------------------------------ #
    # Signal handlers                                                    #
    # ------------------------------------------------------------------ #
    async def _handle_signal(self, signal: dict) -> None:
        current_time = time.time()
        self.logger().info(f"Received signal: {signal}")

        # Check max open trades
        if self._max_open_trades_reached():
            self.logger().info(f"Max number of active traders reached ({self.config.max_open_trades}) – skipping signal")
            return

        # Check for duplicate signal
        sig_hash = self._generate_signal_hash(signal)
        if self._is_duplicate_signal(sig_hash):
            self.logger().info(f"Duplicate signal (hash={sig_hash}) – skipped")
            return

        # Check paper trade connector
        if not self._check_paper_trade_connector():
            return

        # Validate signal
        is_valid, error_msg = self._validate_signal(signal)
        if not is_valid:
            self.logger().error(f"Invalid signal: {error_msg} in signal: {signal}")
            return

        try:
            pair = signal["trading_pair"]

            # Check if pair is supported
            if pair not in self._market_data_provider.get_trading_pairs(self.config.connector_name):
                self.logger().info(f"Trading pair {pair} is not supported – skipping signal")
                return
            
            # Check if pair is already being traded
            if self._pair_already_trading(pair):
                self.logger().info(f"Executor for {pair} already active – skipping duplicate pair signal")
                return


            # Calculate position parameters
            params = self._calculate_position_parameters(signal)

            # Create triple barrier config
            tp_barrier = TripleBarrierConfig(
                stop_loss=params["sl_percentage"],
                take_profit=params["tp2_percentage"],
                time_limit=params["trading_time"],
                trailing_stop=TrailingStop(
                    activation_price=params["tp1_percentage"],
                    trailing_delta=params["tp1_percentage"]
                ),
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.MARKET,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            )

            # get current trading pair price
            current_price = self._market_data_provider.get_price_by_type(self.config.connector_name, pair, PriceType.MidPrice)
            
            # Create unique key for this position
            level_key = f"{pair}_{current_time}_{sig_hash}"

            # Create position executor config
            side = TradeType.BUY if signal["side"].upper() == "BUY" else TradeType.SELL
            cfg = PositionExecutorConfig(
                timestamp=current_time,
                connector_name=self.config.connector_name,
                trading_pair=pair,
                side=side,
                amount=params["position_size"],
                entry_price=params["entry_price"],
                triple_barrier_config=tp_barrier,
                leverage=1,
                activation_bounds=[params["activation_pct"], params["activation_pct"]],
                level_id=level_key
            )

            # Register this executor
            self._registry[level_key] = cfg
            self.logger().info(
                f"Registered new executor with level key {level_key} "
                f"{pair} Current price: {current_price} ,"
                f"Buy range:[{cfg.entry_price*(1-cfg.activation_bounds[0])},{cfg.entry_price*(1+cfg.activation_bounds[1])}] , "
                f"TP:{tp_barrier.take_profit *100} %, "
                f"trailing activation: {tp_barrier.trailing_stop.activation_price * 100}%, "
                f"trailing delta: {tp_barrier.trailing_stop.trailing_delta * 100}%, "
                f"time limit: {tp_barrier.time_limit} seconds"
            )

        except Exception as e:
            self.logger().error(f"Error processing signal: {signal} -> {e}", exc_info=True)

    # ------------------------------------------------------------------ #
    # Executor actions                                                   #
    # Called by the control loop to determine actions to take            #
    # ------------------------------------------------------------------ #

    def determine_executor_actions(self) -> List[CreateExecutorAction]:
        actions: List[CreateExecutorAction] = []

        """Remove registry entries for closed positions or positions that never traded."""
        now = time.time()
        # Remove closed and idle executors from registry
        for ex in list(self.executors_info):
            level_id = ex.config.level_id
            # Case 1 – already closed
            if ex.is_done and level_id in self._registry:
                self._registry.pop(level_id, None)
                self.logger().info(f"Cleaned registry for closed executor {level_id}")
                continue
    
            # Case 2 – never traded and idle too long
            idle_time = self.market_data_provider.time() - ex.timestamp
            if not ex.is_trading and ex.is_active and idle_time > self.config.position_max_initial_idle_time:
                self._registry.pop(level_id, None)
                actions.append(StopExecutorAction(
                     controller_id=self.config.id,
                     keep_position=False,
                     executor_id=ex.id
                ))

                self.logger().info(
                    f"Cancelled never-traded position {level_id} "
                    f"after {idle_time:.0f}s"
                )

        # Create new actions for still-active configs
        for level_id, cfg in list(self._registry.items()):
            if not any(ex.config.level_id == level_id for ex in self.executors_info):
                actions.append(CreateExecutorAction(
                    controller_id=self.config.id,
                    executor_config=cfg
                ))

        return actions
