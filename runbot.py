#!/usr/bin/env python3
"""
EdgeX Futures Trading Bot - Using Official EdgeX Python SDK
"""

import sys
import os
import time
import csv
import logging
import argparse
import asyncio
import json
import traceback
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import pytz
import dotenv

# Import EdgeX SDK from local folder
from edgex_sdk import Client, OrderSide, WebSocketManager, CancelOrderParams, GetOrderBookDepthParams, GetActiveOrderParams

dotenv.load_dotenv()


@dataclass
class TradingConfig:
    """Configuration class for trading parameters."""
    contract_id: str = '10000002'  # EdgeX contract ID (ETH-USDT equivalent)
    quantity: float = 0.1
    take_profit: float = 0.9
    direction: str = 'buy'
    max_orders: int = 40
    wait_time: int = 450

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'buy' if self.direction == "sell" else 'sell'


@dataclass
class OrderMonitor:
    """Thread-safe order monitoring state."""
    order_id: Optional[str] = None
    filled: bool = False
    filled_price: Optional[float] = None
    filled_qty: float = 0.0

    def reset(self):
        """Reset the monitor state."""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = 0.0


class TradingLogger:
    """Enhanced logging with structured output and error handling."""

    def __init__(self, contract_id: str, log_to_console: bool = False):
        self.contract_id = contract_id
        self.log_file = f"{contract_id}_transactions_log.csv"
        self.debug_log_file = f"{contract_id}_bot_activity.log"
        # Force Europe/Paris timezone for logging timestamps
        self.timezone = pytz.timezone('Europe/Paris')
        self.logger = self._setup_logger(log_to_console)

    def _setup_logger(self, log_to_console: bool) -> logging.Logger:
        """Setup the logger with proper configuration."""
        logger = logging.getLogger(f"trading_bot_{self.contract_id}")
        logger.setLevel(logging.INFO)

        # Prevent duplicate handlers
        if logger.handlers:
            return logger

        class TimeZoneFormatter(logging.Formatter):
            def __init__(self, fmt=None, datefmt=None, tz=None):
                super().__init__(fmt=fmt, datefmt=datefmt)
                self.tz = tz

            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, tz=self.tz)
                if datefmt:
                    return dt.strftime(datefmt)
                return dt.isoformat()

        formatter = TimeZoneFormatter(
            "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            tz=self.timezone
        )

        # File handler
        file_handler = logging.FileHandler(self.debug_log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler if requested
        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def log(self, message: str, level: str = "INFO"):
        """Log a message with the specified level."""
        if level.upper() == "DEBUG":
            self.logger.debug(message)
        elif level.upper() == "INFO":
            self.logger.info(message)
        elif level.upper() == "WARNING":
            self.logger.warning(message)
        elif level.upper() == "ERROR":
            self.logger.error(message)
        else:
            self.logger.info(message)

    def log_transaction(self, order_id: str, side: str, quantity: float, price: float, status: str):
        """Log a transaction to CSV file."""
        try:
            timestamp = datetime.now(self.timezone).strftime("%Y-%m-%d %H:%M:%S")
            row = [timestamp, order_id, side, quantity, price, status]

            # Check if file exists to write headers
            file_exists = os.path.isfile(self.log_file)

            with open(self.log_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(['Timestamp', 'OrderID', 'Side', 'Quantity', 'Price', 'Status'])
                writer.writerow(row)

        except Exception as e:
            self.log(f"Failed to log transaction: {e}", "ERROR")


class EdgeXTradingBot:
    """EdgeX Futures Trading Bot - Main trading logic using official SDK."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.logger = TradingLogger(config.contract_id, log_to_console=True)

        # EdgeX credentials from environment
        self.account_id = os.getenv('EDGEX_ACCOUNT_ID')
        self.stark_private_key = os.getenv('EDGEX_STARK_PRIVATE_KEY')
        self.base_url = os.getenv('EDGEX_BASE_URL', 'https://pro.edgex.exchange')
        self.ws_url = os.getenv('EDGEX_WS_URL', 'wss://quote.edgex.exchange')

        if not self.account_id or not self.stark_private_key:
            raise ValueError("EDGEX_ACCOUNT_ID and EDGEX_STARK_PRIVATE_KEY must be set in environment variables")

        # Initialize EdgeX client using official SDK
        self.client = Client(
            base_url=self.base_url,
            account_id=int(self.account_id),
            stark_private_key=self.stark_private_key
        )

        # Initialize WebSocket manager using official SDK
        self.ws_manager = WebSocketManager(
            base_url=self.ws_url,
            account_id=int(self.account_id),
            stark_pri_key=self.stark_private_key
        )

        # Trading state
        self.active_close_orders = []
        self.last_close_orders = 0
        self.last_open_order_time = 0
        self.last_log_time = 0
        self.current_order_status = "PENDING"
        self.order_filled_event = asyncio.Event()
        self.shutdown_requested = False

        # Register order callback
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True
        
        try:
            # Close HTTP client session first
            if hasattr(self, 'client') and self.client:
                self.logger.log("Closing HTTP client session...", "INFO")
                await self.client.close()
            
            # Disconnect WebSocket connections
            if hasattr(self, 'ws_manager'):
                self.logger.log("Disconnecting WebSocket connections...", "INFO")
                self.ws_manager.disconnect_all()
            
            self.logger.log("Graceful shutdown completed", "INFO")
            
        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Parse the message structure
                if isinstance(message, str):
                    message = json.loads(message)

                # Check if this is a trade-event with ORDER_UPDATE
                if (message.get('type') == 'trade-event' and
                        message.get('content', {}).get('event') == 'ORDER_UPDATE'):

                    # Extract order data from the nested structure
                    content = message.get('content', {})
                    data = content.get('data', {})
                    orders = data.get('order', [])

                    if orders and len(orders) > 0:
                        order = orders[0]  # Get the first order
                        if order.get('contractId') != self.config.contract_id:
                            return
                        order_id = order.get('id')
                        status = order.get('status')
                        if order.get('side') == self.config.close_order_side.upper():
                            order_type = "CLOSE"
                        else:
                            order_type = "OPEN"

                        if status == 'FILLED':
                            if len(data.get('collateral')):
                                self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                                f"{order.get('size')} @ {order.get('price')}", "INFO")

                                # Log the filled transaction to CSV using log_transaction function
                                order_side = order.get('side', '').lower()  # Convert to lowercase for consistency
                                order_size = float(order.get('size', 0))
                                order_price = float(order.get('price', 0))

                                # Use log_transaction to log the filled order
                                self.logger.log_transaction(
                                    order_id=order_id,
                                    side=order_side,
                                    quantity=order_size,
                                    price=order_price,
                                    status=status
                                )

                            self.order_filled_event.set()
                        else:
                            self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                            f"{order.get('size')} @ {order.get('price')}", "INFO")
                    else:
                        self.logger.log(f"[{order_type}] No order data found in message", "WARNING")
                else:
                    self.logger.log(f"Unexpected message format: {message.get('type')}", "DEBUG")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Subscribe to order updates
        self.ws_manager.subscribe_order_update(order_update_handler)

    async def place_open_order(self, contract_id: str, quantity: float, direction: str) -> Dict[str, Any]:
        """Place an open order with EdgeX using official SDK with retry logic for POST_ONLY rejections."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            try:
                depth_params = GetOrderBookDepthParams(contract_id=contract_id, limit=15)
                order_book = await self.client.quote.get_order_book_depth(depth_params)

                # Debug: log the full response structure
                self.logger.log(f"Order book response structure: {type(order_book)}", "DEBUG")
                if isinstance(order_book, dict):
                    self.logger.log(f"Order book keys: {list(order_book.keys())}", "DEBUG")
                    if 'data' in order_book:
                        self.logger.log(f"Data type: {type(order_book['data'])}", "DEBUG")
                        if isinstance(order_book['data'], list):
                            self.logger.log(f"Data list length: {len(order_book['data'])}", "DEBUG")
                            if order_book['data']:
                                self.logger.log(f"First data item: {order_book['data'][0]}", "DEBUG")

                # Handle the response format: {"code": "SUCCESS", "data": [{"asks": [...], "bids": [...]}]}
                if not isinstance(order_book, dict) or 'data' not in order_book:
                    self.logger.log(f"Unexpected order book response format: {type(order_book)}", "ERROR")
                    return {'status': 'error', 'err_msg': 'Unexpected order book response format'}

                order_book_data = order_book['data']
                if not isinstance(order_book_data, list) or len(order_book_data) == 0:
                    self.logger.log(f"Order book data is not a valid list: {type(order_book_data)}", "ERROR")
                    return {'status': 'error', 'err_msg': 'Order book data is not a valid list'}

                # Get the first (and should be only) order book entry
                order_book_entry = order_book_data[0]
                if not isinstance(order_book_entry, dict):
                    self.logger.log(f"Order book entry is not a dict: {type(order_book_entry)}", "ERROR")
                    return {'status': 'error', 'err_msg': 'Order book entry is not a dict'}

                # Extract bids and asks from the entry
                bids = order_book_entry.get('bids', [])
                asks = order_book_entry.get('asks', [])

                if not bids or not asks:
                    self.logger.log("[OPEN] No bid/ask data available in order book", "ERROR")
                    return {'status': 'error', 'err_msg': 'No bid/ask data available'}

                # Best bid is the highest price someone is willing to buy at
                best_bid = float(bids[0]['price']) if bids and len(bids) > 0 else 0
                # Best ask is the lowest price someone is willing to sell at
                best_ask = float(asks[0]['price']) if asks and len(asks) > 0 else 0

                if best_bid <= 0 or best_ask <= 0:
                    return {'status': 'error', 'err_msg': 'Invalid bid/ask prices'}

                # Calculate order price based on direction
                if contract_id == '10000001':
                    price_delta = 0.1
                else:
                    price_delta = 0.01

                if direction == 'buy':
                    # For buy orders, place slightly below best ask to ensure execution
                    order_price = best_ask - price_delta
                    side = OrderSide.BUY
                else:
                    # For sell orders, place slightly above best bid to ensure execution
                    order_price = best_bid + price_delta
                    side = OrderSide.SELL

                # Place the order using official SDK (post-only to ensure maker order)
                order_result = await self.client.create_limit_order(
                    contract_id=contract_id,
                    size=str(quantity),
                    price=str(round(order_price, 2)),
                    side=side,
                    post_only=True
                )

                if not order_result or 'data' not in order_result:
                    self.logger.log("[OPEN] Failed to place open order", "ERROR")
                    return {'status': 'error', 'err_msg': 'Failed to place order'}

                # Extract order ID from response
                order_id = order_result['data'].get('orderId')
                if not order_id:
                    return {'status': 'error', 'err_msg': 'No order ID in response'}

                # Check order status after a short delay to see if it was rejected
                await asyncio.sleep(0.01)
                order_info = await self.get_order_info(order_id)

                if order_info and 'data' in order_info:
                    order_data = order_info['data']
                    status = order_data.get('status')

                    if status == 'CANCELED':
                        cancel_reason = order_data.get('cancelReason', 'UNKNOWN')
                        self.logger.log(
                            f"Order {order_id} was canceled. Reason: {cancel_reason}. "
                            f"Retrying... (attempt {retry_count + 1}/{max_retries})",
                            "WARNING"
                        )

                        if retry_count < max_retries - 1:
                            retry_count += 1
                            continue
                        else:
                            self.logger.log("[OPEN] Max retries reached for order placement", "ERROR")
                            return {'status': 'error', 'err_msg': f'Order rejected after {max_retries} attempts'}
                    elif status in ['OPEN', 'PARTIALLY_FILLED', 'FILLED']:
                        # Order successfully placed
                        return {
                            'status': 'ok',
                            'data': {
                                'order_id': order_id,
                                'side': side.value,
                                'size': quantity,
                                'price': order_price,
                                'status': status
                            }
                        }
                    else:
                        self.logger.log(f"[OPEN] Order {order_id} has unexpected status: {status}", "WARNING")
                        return {'status': 'error', 'err_msg': f'Unexpected order status: {status}'}
                else:
                    self.logger.log(f"[OPEN] Could not retrieve order info for {order_id}", "WARNING")
                    # Assume order is successful if we can't get info
                    return {
                        'status': 'ok',
                        'data': {
                            'order_id': order_id,
                            'side': side.value,
                            'size': quantity,
                            'price': order_price
                        }
                    }

            except Exception as e:
                self.logger.log(f"[OPEN] Error placing open order (attempt {retry_count + 1}): {e}", "ERROR")
                if retry_count < max_retries - 1:
                    retry_count += 1
                    await asyncio.sleep(0.1)  # Wait before retry
                    continue
                else:
                    return {'status': 'error', 'err_msg': str(e)}

        return {'status': 'error', 'err_msg': 'Max retries exceeded'}

    async def place_close_order(self, contract_id: str, quantity: float, price: float, side: str) -> Dict[str, Any]:
        """Place a close order with EdgeX using official SDK with retry logic for POST_ONLY rejections."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Get current market prices to adjust order price if needed
                depth_params = GetOrderBookDepthParams(contract_id=contract_id, limit=15)
                order_book = await self.client.quote.get_order_book_depth(depth_params)

                if not isinstance(order_book, dict) or 'data' not in order_book:
                    self.logger.log("[CLOSE] Failed to get order book for close order price adjustment", "ERROR")
                    return {'status': 'error', 'err_msg': 'Failed to get order book'}

                order_book_data = order_book['data']
                if not isinstance(order_book_data, list) or len(order_book_data) == 0:
                    self.logger.log("[CLOSE] Order book data is not valid for close order", "ERROR")
                    return {'status': 'error', 'err_msg': 'Invalid order book data'}

                # Get the first order book entry
                order_book_entry = order_book_data[0]
                bids = order_book_entry.get('bids', [])
                asks = order_book_entry.get('asks', [])

                if not bids or not asks:
                    self.logger.log("[CLOSE] No bid/ask data available for close order", "ERROR")
                    return {'status': 'error', 'err_msg': 'No bid/ask data available'}

                # Get best bid and ask prices
                best_bid = float(bids[0]['price']) if bids and len(bids) > 0 else 0
                best_ask = float(asks[0]['price']) if asks and len(asks) > 0 else 0

                if best_bid <= 0 or best_ask <= 0:
                    self.logger.log(f"[CLOSE] Invalid bid/ask prices for close order: bid={best_bid}, ask={best_ask}", "ERROR")
                    return {'status': 'error', 'err_msg': 'Invalid bid/ask prices'}

                # Convert side string to OrderSide enum
                order_side = OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL

                # Adjust order price based on market conditions and side
                adjusted_price = price
                if contract_id == '10000001':
                    price_delta = 0.1
                else:
                    price_delta = 0.01
                if side.lower() == 'sell':
                    # For sell orders, ensure price is above best bid to be a maker order
                    if price <= best_bid:
                        adjusted_price = best_bid + price_delta
                elif side.lower() == 'buy':
                    # For buy orders, ensure price is below best ask to be a maker order
                    if price >= best_ask:
                        adjusted_price = best_ask - price_delta

                # Place the order using official SDK (post-only to avoid taker fees)
                order_result = await self.client.create_limit_order(
                    contract_id=contract_id,
                    size=str(quantity),
                    price=str(round(adjusted_price, 2)),
                    side=order_side,
                    post_only=True
                )

                if not order_result or 'data' not in order_result:
                    self.logger.log("[CLOSE] Failed to place close order", "ERROR")
                    return {'status': 'error', 'err_msg': 'Failed to place order'}

                # Extract order ID from response
                order_id = order_result['data'].get('orderId')
                if not order_id:
                    return {'status': 'error', 'err_msg': 'No order ID in response'}

                # Check order status after a short delay to see if it was rejected
                await asyncio.sleep(0.01)
                order_info = await self.get_order_info(order_id)

                if order_info and 'data' in order_info:
                    order_data = order_info['data']
                    status = order_data.get('status')

                    if status == 'CANCELED':
                        cancel_reason = order_data.get('cancelReason', 'UNKNOWN')
                        self.logger.log(
                            f"[CLOSE] Close order {order_id} was canceled. Reason: {cancel_reason}. "
                            f"Retrying... (attempt {retry_count + 1}/{max_retries})",
                            "WARNING"
                        )

                        if retry_count < max_retries - 1:
                            retry_count += 1
                            continue
                        else:
                            self.logger.log("[CLOSE] Max retries reached for close order placement", "ERROR")
                            return {'status': 'error', 'err_msg': f'Close order rejected after {max_retries} attempts'}
                    elif status in ['OPEN', 'PARTIALLY_FILLED', 'FILLED']:
                        self.logger.log(f"[CLOSE] [{order_id}] Order placed: {quantity} @ {price}", "INFO")
                        # Order successfully placed
                        return {
                            'status': 'ok',
                            'data': {
                                'order_id': order_id,
                                'side': side,
                                'size': quantity,
                                'price': adjusted_price,
                                'status': status
                            }
                        }
                    else:
                        self.logger.log(f"[CLOSE] Close order {order_id} has unexpected status: {status}", "WARNING")
                        return {'status': 'error', 'err_msg': f'Unexpected close order status: {status}'}
                else:
                    self.logger.log(f"[CLOSE] Could not retrieve close order info for {order_id}", "WARNING")
                    # Assume order is successful if we can't get info
                    return {
                        'status': 'ok',
                        'data': {
                            'order_id': order_id,
                            'side': side,
                            'size': quantity,
                            'price': adjusted_price
                        }
                    }

            except Exception as e:
                self.logger.log(f"[CLOSE] Error placing close order (attempt {retry_count + 1}): {e}", "ERROR")
                if retry_count < max_retries - 1:
                    retry_count += 1
                    await asyncio.sleep(0.1)  # Wait before retry
                    continue
                else:
                    return {'status': 'error', 'err_msg': str(e)}

        return {'status': 'error', 'err_msg': 'Max retries exceeded for close order'}

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an order with EdgeX using official SDK."""
        try:
            # Create cancel parameters using official SDK
            cancel_params = CancelOrderParams(order_id=order_id)

            # Cancel the order using official SDK
            cancel_result = await self.client.cancel_order(cancel_params)

            if not cancel_result or 'data' not in cancel_result:
                self.logger.log(f"Failed to cancel order {order_id}", "ERROR")
                return {'status': 'error', 'err_msg': 'Failed to cancel order'}

            return {'status': 'ok', 'data': cancel_result}

        except Exception as e:
            self.logger.log(f"Error canceling order {order_id}: {e}", "ERROR")
            return {'status': 'error', 'err_msg': str(e)}

    async def get_order_info(self, order_id: str) -> Dict[str, Any]:
        """Get order information from EdgeX using official SDK."""
        try:
            # Use the newly created get_order_by_id method
            order_result = await self.client.order.get_order_by_id(order_id_list=[order_id])

            if not order_result or 'data' not in order_result:
                return {}

            # The API returns a list of orders, get the first (and should be only) one
            order_list = order_result['data']
            if order_list and len(order_list) > 0:
                return {'data': order_list[0]}

            return {}

        except Exception as e:
            self.logger.log(f"Error getting order info for {order_id}: {e}", "ERROR")
            return {}

    async def get_active_orders(self, contract_id: str) -> List[Dict[str, Any]]:
        """Get active orders for a contract using official SDK."""
        try:
            # Get active orders using official SDK
            params = GetActiveOrderParams(size="100", offset_data="")
            active_orders = await self.client.get_active_orders(params)

            if not active_orders or 'data' not in active_orders:
                return []

            # Filter orders for the specific contract and ensure they are dictionaries
            # The API returns orders under 'dataList' key, not 'orderList'
            order_list = active_orders['data'].get('dataList', [])
            contract_orders = []

            for order in order_list:
                if isinstance(order, dict) and order.get('contractId') == contract_id:
                    contract_orders.append(order)

            return contract_orders

        except Exception as e:
            self.logger.log(f"Error getting active orders: {e}", "ERROR")
            return []

    async def get_account_positions(self) -> Dict[str, Any]:
        """Get account positions using official SDK."""
        try:
            positions = await self.client.get_account_positions()
            return positions
        except Exception as e:
            self.logger.log(f"Error getting positions: {e}", "ERROR")
            return {}

    def _calculate_wait_time(self) -> float:
        """Calculate wait time between orders."""
        cool_down_time = self.config.wait_time

        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return 0

        self.last_close_orders = len(self.active_close_orders)
        if len(self.active_close_orders) >= self.config.max_orders:
            return 1

        if len(self.active_close_orders) / self.config.max_orders >= 2/3:
            cool_down_time = 2 * self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/3:
            cool_down_time = self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/6:
            cool_down_time = self.config.wait_time / 2
        else:
            cool_down_time = 60

        if time.time() - self.last_open_order_time > cool_down_time:
            return 0
        else:
            return 1

    async def _place_and_monitor_open_order(self) -> bool:
        """Place an order and monitor its execution."""
        try:
            # Reset state before placing order
            self.order_filled_event.clear()

            # Place the order
            order = await self.place_open_order(
                self.config.contract_id,
                self.config.quantity,
                self.config.direction
            )

            if order.get('status') != 'ok':
                self.logger.log(f"Failed to place order: {order}", "ERROR")
                return False

            self.last_open_order_time = time.time()

            # Wait for fill or timeout
            if order.get('data').get('status') != 'FILLED':
                try:
                    await asyncio.wait_for(self.order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass

            # Handle order result
            return await self._handle_order_result(order)

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            return False

    async def _handle_order_result(self, order: Dict[str, Any]) -> bool:
        """Handle the result of an order placement."""
        order_id = order['data']['order_id']

        # Get current order status
        order_info = await self.get_order_info(order_id)

        # Extract status from EdgeX response
        status = order_info.get('data', {}).get('status', 'UNKNOWN')

        if status == 'FILLED':
            self.current_order_status = "FILLED"

            # Place close order
            filled_price = float(order_info['data'].get('price', 0))
            if filled_price > 0:
                close_side = self.config.close_order_side
                if close_side == 'sell':
                    close_price = filled_price + self.config.take_profit
                else:
                    close_price = filled_price - self.config.take_profit
                self.logger.log(f"[OPEN] [{order_id}] Order placed and FILLED: {self.config.quantity} @ {filled_price}", "INFO")
                close_order = await self.place_close_order(
                    self.config.contract_id,
                    self.config.quantity,
                    close_price,
                    close_side
                )

                if close_order.get('status') != 'ok':
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order}", "ERROR")

            return True

        elif status in ['OPEN', 'PARTIALLY_FILLED']:
            # Cancel the order if it's still open
            try:
                cancel_result = await self.cancel_order(order_id)
                if cancel_result.get('status') == 'ok':
                    self.current_order_status = "CANCELED"
                else:
                    self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result}", "ERROR")

            except Exception as e:
                self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

            order_info = await self.get_order_info(order_id)
            filled_amount = float(order_info['data'].get('cumFillSize', 0))
            filled_price = float(order_info['data'].get('price', 0))
            self.logger.log(f"[OPEN] [{order_id}] Order placed and PARTIALLY FILLED: {filled_amount} @ {filled_price}", "INFO")
            if filled_amount > 0:
                close_side = self.config.close_order_side
                if close_side == 'sell':
                    close_price = filled_price + self.config.take_profit
                else:
                    close_price = filled_price - self.config.take_profit
                close_order = await self.place_close_order(
                    self.config.contract_id,
                    filled_amount,
                    close_price,
                    close_side
                )

                if close_order.get('status') != 'ok':
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order}", "ERROR")

            return True

        return False

    async def _log_status_periodically(self):
        """Log status information periodically, including positions."""
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                # Get active orders
                active_orders = await self.get_active_orders(self.config.contract_id)
                self.logger.log(f"Debug: Retrieved {len(active_orders)} active orders", "DEBUG")

                # Filter close orders with better error handling
                self.active_close_orders = []
                for order in active_orders:
                    try:
                        if isinstance(order, dict):
                            order_side = order.get('side', '')
                            if order_side == self.config.close_order_side.upper():
                                self.active_close_orders.append(
                                    {'id': order.get('id'),
                                     'price': order.get('price'),
                                     'size': order.get('size')})
                        else:
                            self.logger.log(f"Debug: Skipping non-dict order: {type(order)}", "DEBUG")
                    except Exception as e:
                        self.logger.log(f"Debug: Error processing order {order}: {e}", "DEBUG")

                # Get positions
                positions_data = await self.get_account_positions()

                if not positions_data or 'data' not in positions_data:
                    self.logger.log("Failed to get positions", "WARNING")
                    position_amt = 0
                else:
                    # The API returns positions under data.positionList
                    positions = positions_data.get('data', {}).get('positionList', [])
                    if positions:
                        # Find position for current contract
                        position = None
                        for p in positions:
                            if isinstance(p, dict) and p.get('contractId') == self.config.contract_id:
                                position = p
                                break

                        if position:
                            position_amt = abs(float(position.get('openSize', 0)))
                        else:
                            position_amt = 0
                    else:
                        position_amt = 0

                # Calculate active closing amount
                active_close_amount = sum(
                    float(order.get('size', 0))
                    for order in self.active_close_orders
                    if isinstance(order, dict)
                )

                self.logger.log(f"Current Position: {position_amt} | Active closing amount: {active_close_amount}")

                # Check for position mismatch
                if abs(position_amt - active_close_amount) > (2 * self.config.quantity):
                    self.logger.log("ERROR: Position mismatch detected", "ERROR")
                    self.logger.log("###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n", "ERROR")
                    self.logger.log("Please manually rebalance your position and take-profit orders", "ERROR")
                    self.logger.log("请手动平衡当前仓位和正在关闭的仓位", "ERROR")
                    self.logger.log(f"current position: {position_amt} | active closing amount: {active_close_amount}\n", "ERROR")
                    self.logger.log("###### ERROR ###### ERROR ###### ERROR ###### ERROR #####", "ERROR")
                    if not self.shutdown_requested:
                        self.shutdown_requested = True
                    return

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            self.last_log_time = time.time()
            print("--------------------------------")

    async def run(self):
        """Main trading loop."""
        try:
            # Connect to WebSocket - only private is needed for order updates
            # self.ws_manager.connect_public()  # Removed - causes duplicate order updates
            self.ws_manager.connect_private()

            # Main trading loop
            while not self.shutdown_requested:
                # Update active orders
                active_orders = await self.get_active_orders(self.config.contract_id)

                # Filter close orders with better error handling
                self.active_close_orders = []

                for order in active_orders:
                    try:
                        if isinstance(order, dict):
                            order_side = order.get('side', '')
                            order_id = order.get('id', 'Unknown')
                            self.logger.log(
                                f"Processing order {order_id}: side={order_side}, "
                                f"close_order_side={self.config.close_order_side.upper()}", "DEBUG"
                            )
                            if order_side == self.config.close_order_side.upper():
                                close_order = {
                                    'id': order.get('id'),
                                    'price': order.get('price'),
                                    'size': order.get('size')
                                }
                                self.active_close_orders.append(close_order)
                                self.logger.log(f"Added close order: {close_order}", "DEBUG")
                    except Exception as e:
                        self.logger.log(f"Debug: Error processing order in main loop: {e}", "DEBUG")

                # Periodic logging
                await self._log_status_periodically()
                wait_time = self._calculate_wait_time()

                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    await self._place_and_monitor_open_order()
                    self.last_close_orders += 1

        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(traceback.format_exc(), "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                # Close HTTP client session
                if hasattr(self, 'client') and self.client:
                    await self.client.close()
            except Exception as e:
                self.logger.log(f"Error closing HTTP client session: {e}", "ERROR")
            
            try:
                # Close WebSocket connections
                if hasattr(self, 'ws_manager'):
                    self.ws_manager.disconnect_all()
            except Exception as e:
                self.logger.log(f"Error closing WebSocket connections: {e}", "ERROR")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='EdgeX Futures Trading Bot')
    parser.add_argument('--contract-id', type=str, default='10000002',
                        help='EdgeX contract ID (default: 10000002 for ETH-USDT)')
    parser.add_argument('--quantity', type=float, default=0.1,
                        help='Order quantity (default: 0.1)')
    parser.add_argument('--take-profit', type=float, default=0.9,
                        help='Take profit in USDT (default: 0.9)')
    parser.add_argument('--direction', type=str, default='buy',
                        help='Direction of the bot (default: buy)')
    parser.add_argument('--max-orders', type=int, default=40,
                        help='Maximum number of active orders (default: 40)')
    parser.add_argument('--wait-time', type=int, default=450,
                        help='Wait time between orders in seconds (default: 450)')
    return parser.parse_args()


async def main():
    """Main entry point."""
    args = parse_arguments()

    # Create configuration
    config = TradingConfig(
        contract_id=args.contract_id,
        quantity=args.quantity,
        take_profit=args.take_profit,
        direction=args.direction,
        max_orders=args.max_orders,
        wait_time=args.wait_time
    )

    # Create and run the bot
    bot = EdgeXTradingBot(config)
    try:
        await bot.run()
    except Exception as e:
        print(f"Bot execution failed: {e}")
        # The bot's run method already handles graceful shutdown
        raise


if __name__ == "__main__":
    asyncio.run(main())
