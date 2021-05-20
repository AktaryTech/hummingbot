import json
import logging
import math
import time
import asyncio
import aiohttp

from decimal import Decimal
from typing import Optional, List, Dict, Any, AsyncIterable
from async_timeout import timeout

from hummingbot.connector.exchange_base import ExchangeBase, s_decimal_NaN
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.event.events import (
    OrderType, OrderCancelledEvent, TradeType, TradeFee, MarketEvent, BuyOrderCreatedEvent, SellOrderCreatedEvent,
    MarketOrderFailureEvent, BuyOrderCompletedEvent, SellOrderCompletedEvent, OrderFilledEvent
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import estimate_fee

from hummingbot.connector.exchange.zebpay.zebpay_auth import ZebpayAuth
from hummingbot.connector.exchange.zebpay.zebpay_in_flight_order import ZebpayInFlightOrder
from hummingbot.connector.exchange.zebpay.zebpay_order_book_tracker import ZebpayOrderBookTracker
from hummingbot.connector.exchange.zebpay.zebpay_user_stream_tracker import ZebpayUserStreamTracker
from hummingbot.connector.exchange.zebpay.zebpay_utils import (EXCHANGE_NAME, get_new_client_order_id, DEBUG,
                                                               HUMMINGBOT_GAS_LOOKUP)
from hummingbot.connector.exchange.zebpay.zebpay_resolve import (
    get_zebpay_rest_url, set_domain, get_throttler
)
from hummingbot.core.utils import eth_gas_station_lookup, async_ttl_cache
from hummingbot.logger import HummingbotLogger

s_decimal_0 = Decimal("0.0")
ie_logger = None


class ZebpayExchange(ExchangeBase):

    SHORT_POLL_INTERVAL = 11.0
    LONG_POLL_INTERVAL = 120.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 45.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global ze_logger
        if ze_logger is None:
            ze_logger = logging.getLogger(__name__)
        return ze_logger

    def __init__(self,
                 zebpay_client_id: str,     # TODO: Confirm validity of client id/secret and API secret as auth params
                 zebpay_client_secret: str,
                 zebpay_api_secret: str,
                 user_country: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain="com"):
        """
        :param zebpay_client_id: The client ID to connect to private zebpay APIs.
        :param zebpay_client_secret: The client secret.
        :param zebpay_api_secret: The API secret to generate the hash signature for authentication.
        :param user_country: The country the Zebpay user is from - affects the available trading-pairs/rules
        :param trading_pairs: The market trading pairs which to track order book data.
        :param trading_required: Whether actual trading is needed.
        """
        self._domain = domain
        self._country = user_country
        set_domain(domain)
        super().__init__()
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._zebpay_auth: ZebpayAuth = ZebpayAuth(zebpay_client_id, zebpay_client_secret, zebpay_api_secret)
        self._account_available_balances = {}  # Dict[asset_name:str, Decimal]
        self._order_book_tracker = ZebpayOrderBookTracker(trading_pairs=trading_pairs, domain=domain)
        self._user_stream_tracker = ZebpayUserStreamTracker(self._zebpay_auth, trading_pairs, domain=domain)
        self._user_stream_tracker_task = None
        self._ev_loop = asyncio.get_event_loop()
        self._shared_client: Optional[aiohttp.ClientSession] = None
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._in_flight_orders = {}  # Dict[client_order_id:str, ZebpayInFlightOrder]
        self._order_not_found_records = {}  # Dict[client_order_id:str, count:int]
        self._trading_rules = {}  # Dict[trading_pair:str, TradingRule]
        self._status_polling_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._last_poll_timestamp = 0
        self._exchange_info = None  # stores info about the exchange. Periodically polled from GET /v1/exchange
        self._market_info = None    # stores info about the markets. Periodically polled from GET /v1/markets
        # self._throttler_public_endpoint = Throttler(rate_limit=(2, 1.0))  # rate_limit=(weight, t_period)
        # self._throttler_user_endpoint = Throttler(rate_limit=(3, 1.0))  # rate_limit=(weight, t_period)
        # self._throttler_trades_endpoint = Throttler(rate_limit=(4, 1.0))  # rate_limit=(weight, t_period)
        self._order_lock = asyncio.Lock()  # exclusive access for modifying orders

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        """Returns the trading rules associated with Zebpay orders/trades"""
        return self._trading_rules

    @property
    def name(self) -> str:
        """Returns the exchange name"""
        if self._domain == "com":  # prod with ETH blockchain
            return EXCHANGE_NAME
        else:
            return f"zebpay_sandbox"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        """Returns the order books of all tracked trading pairs"""
        return self._order_book_tracker.order_books

    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        A dictionary of statuses of various connector's components.
        """
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0,
            "user_stream_initialized":
                self._user_stream_tracker.data_source.last_recv_time > 0 if self._trading_required else True,
        }

    @property
    def ready(self) -> bool:
        """
        :return True when all statuses pass, this might take 5-10 seconds for all the connector's components and
        services to be ready.
        """
        return all(self.status_dict.values())

    @property
    def limit_orders(self) -> List[LimitOrder]:
        """Returns a list of active limit orders being tracked"""
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def in_flight_orders(self) -> Dict[str, ZebpayInFlightOrder]:
        """ Returns a list of all active orders being tracked """
        return self._in_flight_orders

    @property
    def tracking_states(self) -> Dict[str, any]:
        """
        :return active in-flight orders in json format, is used to save in sqlite db.
        """
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
            if not value.is_done
        }

    async def _http_client(self) -> aiohttp.ClientSession:
        """
        :returns: Shared client session instance
        """
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        """
        Restore in-flight orders from saved tracking states, this is so the connector can pick up on where it left off
        when it disconnects.
        :param saved_states: The saved tracking_states.
        """
        self._in_flight_orders.update({
            key: ZebpayInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector.
        Note that Market order type is no longer required and will not be used.
        """
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def start(self, clock: Clock, timestamp: float):
        """
        This function is called automatically by the clock.
        """
        super().start(clock, timestamp)

    def stop(self, clock: Clock):
        """
        This function is called automatically by the clock.
        """
        super().stop(clock)

    def get_order_price_quantum(self, trading_pair: str, price: Decimal) -> Decimal:
        """Provides the Zebpay standard minimum price increment across all trading pairs"""
        trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal) -> Decimal:
        """Provides the Zebpay standard minimum order increment across all trading pairs"""
        trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    async def start_network(self):
        await self.stop_network()
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    async def stop_network(self):
        self._order_book_tracker.stop()

        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
        self._status_polling_task = self._trading_rules_polling_task = \
            self._user_stream_tracker_task = self._user_stream_event_listener_task = None

    async def check_network(self) -> NetworkStatus:
        """
        This function is required by NetworkIterator base class and is called periodically to check
        the network connection. Simply ping the network (or call any light weight public API).
        """
        try:
            await self.get_ping()
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    async def _trading_rules_polling_loop(self):
        """
        Periodically update trading rule.
        """
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().network(f"Unexpected error while fetching trading rules. Error: {str(e)}",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Zebpay. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    async def _update_trading_rules(self):
        market_info = self._market_info if self._market_info else await self.get_market_info_from_api()
        self._trading_rules.clear()
        self._trading_rules = self._format_trading_rules(market_info)

    def _format_trading_rules(self, market_info: List[Dict[str, Any]]) -> Dict[str, TradingRule]:
        """
        Converts json API response into a dictionary of trading rules.
        :param exchange_info: The json API response for exchange rules
        :param market_info: The json API response for trading pairs
        :return A dictionary of trading rules.
        Exchange Response Example:
        {
                    "buy": "0",
                    "sell": "0",
                    "volume": 0,
                    "pricechange": "0.00",
                    "24hoursHigh": "0",
                    "24hoursLow": "0",
                    "pair": "BTC-AUD",
                    "virtualCurrency": "BTC",
                    "currency": "AUD"
        },
        ...
        """
        rules = {}
        for trading_pair in market_info:
            trading_pair_name = trading_pair["tradePairName"]
            try:
                rules[trading_pair_name] = TradingRule(trading_pair=trading_pair_name,
                                                  min_order_size=trading_pair["tradeMinimumAmount"],
                                                  max_order_size=trading_pair["tradeMaximumAmount"],
                                                  min_price_increment=trading_pair["tickSize"],
                                                  min_base_amount_increment=trading_pair["tickSize"],
                                                  )
            except Exception:
                self.logger().error(f"Error parsing the exchange rules for {trading_pair_name}. Skipping.",
                                    exc_info=True)
        return rules

    async def _api_request(self,
                           method: str,
                           path_url: str,
                           params: Dict[str, Any] = {},
                           is_auth_required: bool = False) -> Dict[str, Any]:
        """
        Sends an aiohttp request and waits for a response.
        :param method: The HTTP method, e.g. get or post
        :param path_url: The path url or the API end point
        :param is_auth_required: Whether an authentication is required, when True the function will add encrypted
        signature to the request.
        :returns A response in json format.
        """
        url = f"{get_zebpay_rest_url()}/{path_url}"
        client = await self._http_client()
        if is_auth_required:
            headers = self.zebpay_auth.get_headers()
        else:
            headers = {"Content-Type": "application/json"}

        if method == "get":
            get_json = json.dumps(params)
            response = await client.get(url, data=get_json, headers=headers)
        elif method == "post" or method == "delete":
            post_json = json.dumps(params)
            response = await client.post(url, data=post_json, headers=headers)
        else:
            raise NotImplementedError

        try:
            parsed_response = json.loads(await response.text())
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. Error: {str(e)}")
        if response.status != 200:
            raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}. "
                          f"Message: {parsed_response}")
        if parsed_response["code"] != 0:
            raise IOError(f"{url} API call failed, response: {parsed_response}")
        # print(f"REQUEST: {method} {path_url} {params}")
        # print(f"RESPONSE: {parsed_response}")
        return parsed_response

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Buys an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for BuyOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to buy from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        client_order_id: str = get_new_client_order_id(True, trading_pair)
        safe_ensure_future(self._create_order(TradeType.BUY, client_order_id, trading_pair, amount, order_type, price))
        return client_order_id

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Sells an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for SellOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to sell from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        client_order_id: str = get_new_client_order_id(False, trading_pair)
        safe_ensure_future(self._create_order(TradeType.SELL, client_order_id, trading_pair, amount, order_type, price))
        return client_order_id

    def cancel(self, trading_pair: str, client_order_id: str):
        """
        Cancel an order. This function returns immediately.
        To get the cancellation result, you'll have to wait for OrderCancelledEvent.
        :param trading_pair: The market (e.g. BTC-USDT) of the order.
        :param client_order_id: The internal order id
        """
        self.logger().info("Cancellation has been called for")
        order_cancellation = safe_ensure_future(self._execute_cancel(trading_pair, client_order_id))
        return order_cancellation

    async def _execute_cancel(self, trading_pair: str, client_order_id: str) -> str:
        """
        Executes order cancellation process by first calling cancel-order API. The API result doesn't confirm whether
        the cancellation is successful, it simply states it receives the request.
        :param trading_pair: The market trading pair
        :param client_order_id: The internal order id
        order.last_state to change to CANCELED
        """

        # Must call _api_request with exchange_order_id
        async with self._order_lock:
            self.logger().warning(f'entering _execute_cancel({trading_pair}, {client_order_id})')
            try:
                tracked_order = self._in_flight_orders.get(client_order_id)
                if tracked_order is None:
                    raise IOError(f"Failed to cancel order - {client_order_id}: order not found.")
                exchange_order_id = tracked_order.exchange_order_id
                response = await self.delete_order(exchange_order_id)
                if response["statusDescription"] == "success":
                    self.logger().info(f"Successfully cancelled order:{client_order_id}. "
                                       f"exchange id:{exchange_order_id}")
                    self.stop_tracking_order(client_order_id)
                    self.trigger_event(MarketEvent.OrderCancelled,
                                       OrderCancelledEvent(
                                           self.current_timestamp,
                                           client_order_id,
                                           tracked_order.exchange_order_id))
                    tracked_order.cancelled_event.set()
                    if DEBUG:
                        self.logger().warning(f'successfully exiting _execute_cancel for {client_order_id}, '
                                              f'exchange_order_id: {exchange_order_id}')
                    return client_order_id
                else:
                    raise IOError(f"delete_order({client_order_id}) tracked with exchange id: {exchange_order_id} was"
                                  f"unsuccessful.")
            except IOError as e:
                self.logger().error(f"_execute_cancel error: order {client_order_id} does not exist on Zebpay. "
                                    f"No cancellation performed: {str(e)}")
                if "order not found" in str(e).lower():
                    # The order was never there to begin with. So cancelling it is a no-op but semantically successful.
                    self.stop_tracking_order(client_order_id)
                    self.trigger_event(MarketEvent.OrderCancelled,
                                       OrderCancelledEvent(
                                           self.current_timestamp,
                                           client_order_id,
                                           tracked_order.exchange_order_id))
                    return client_order_id
                else:
                    self.logger().network(
                        f"Failed to cancel not found order {client_order_id}: {str(e)}",
                        exc_info=True,
                        app_warning_msg=f"Failed to cancel the order {client_order_id} on Zebpay.")
                    raise e
            except asyncio.CancelledError as e:
                self.logger().warning(f'_execute_cancel: About to re-raise CancelledError: {str(e)}')
                raise e
            except Exception as e:
                self.logger().exception(f'_execute_cancel raised unexpected exception: {e}. Details:')
                self.logger().network(
                    f"Failed to cancel order {client_order_id}: {str(e)}",
                    exc_info=True,
                    app_warning_msg=f"Failed to cancel the order {client_order_id} on Zebpay. "
                                    f"Check API key and network connection.")

    # API Calls

    async def list_orders(self) -> List[Dict[str, Any]]:
        """Requests status of all active orders. Returns json data of all orders associated with user account"""
        async with get_throttler().weighted_task(request_weight=1):
            rest_url = get_zebpay_rest_url()
            url = f"{rest_url}/orders"
            result = await self._api_request("get", url, {}, True)
            return result

    async def get_order(self, exchange_order_id: str) -> Dict[str, Any]:
        """Requests order information through API with exchange order Id. Returns json data with order details"""
        async with get_throttler().weighted_task(request_weight=1):
            if DEBUG:
                self.logger().warning(f'<|<|<|<|< entering get_order({exchange_order_id})')

            rest_url = get_zebpay_rest_url()
            url = f"{rest_url}orders"
            params = {
                "orderid": exchange_order_id
            }
            result = await self._api_request("get", url, params, True)
            return result

    async def delete_order(self, exchange_order_id: str):
        """
        Deletes an order or all orders associated with a wallet from the Zebpay API.
        Returns json data with order id confirming deletion
        """
        async with get_throttler().weighted_task(request_weight=1):
            rest_url = get_zebpay_rest_url()
            url = f"{rest_url}/orders/{exchange_order_id}"

            result = await self._api_request("delete", url, True)
            return result

    # Zebpay API does not currently support user balance retrieval in the API per the documentation
    async def get_balances_from_api(self) -> List[Dict[str, Any]]:
        """Requests current balances of all assets through API. Returns json data with balance details"""
        async with get_throttler().weighted_task(request_weight=1):
            rest_url = get_zebpay_rest_url()
            url = f"{rest_url}/v1/balances"
            params = {
                "nonce": self._zebpay_auth.generate_nonce(),
                "wallet": self._zebpay_auth.get_wallet_address(),
            }
            auth_dict = self._zebpay_auth.generate_auth_dict(http_method="GET", url=url, params=params)
            session: aiohttp.ClientSession = await self._http_client()
            async with session.get(auth_dict["url"], headers=auth_dict["headers"]) as response:
                if response.status != 200:
                    raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}. {response}")
                data = await response.json()
                return data

    async def get_exchange_info_from_api(self) -> Dict[str, Any]:
        """Requests basic info about zebpay exchange. We are mostly interested in the gas price in gwei"""
        async with get_throttler().weighted_task(request_weight=1):
            rest_url = get_zebpay_rest_url()
            url = f"{rest_url}/market"
            result = await self._api_request("get", url, {}, False)
            return result

    async def get_market_info_from_api(self) -> Dict[str, Any]:
        """Requests all markets (trading pairs) available to Zebpay users."""
        async with get_throttler().weighted_task(request_weight=1):
            rest_url = get_zebpay_rest_url()
            url = f"{rest_url}/api/v1/tradepairs/in"
            result = await self._api_request("get", url, {}, False)
            market_info = result["data"]
            return market_info

    async def _create_order(self,
                            trade_type: TradeType,
                            client_order_id: str,
                            trading_pair: str,
                            amount: Decimal,
                            order_type: OrderType,
                            price: Decimal):
        """
        Calls create-order API end point to place an order, starts tracking the order and triggers order created event.
        :param trade_type: BUY or SELL
        :param client_order_id: Internal order id (also called client_order_id)
        :param trading_pair: The market to place order
        :param amount: The order amount (in base token value)
        :param order_type: The order type (MARKET, LIMIT, etc..)
        :param price: The order price
        """
        async with self._order_lock:
            try:
                if not order_type.is_limit_type():
                    raise Exception(f"Unsupported order type: {order_type}")
                trading_rule = self._trading_rules[trading_pair]

                amount = self.quantize_order_amount(trading_pair, amount)
                price = self.quantize_order_price(trading_pair, price)

                if amount < trading_rule.min_order_size:
                    raise ValueError(f"Buy order amount {amount} is lower than the minimum order size "
                                     f"{trading_rule.min_order_size}. client_order_id: {client_order_id}")

                if amount < trading_rule.max_order_size:
                    raise ValueError(f"Buy order amount {amount} is higher than the maximum order size "
                                     f"{trading_rule.max_order_size}. client_order_id: {client_order_id}")

                if trade_type.value == 1:
                    trade_side = "bid"
                else:
                    trade_type = "ask"

                url = f"{get_zebpay_rest_url()}/orders"
                api_params = {
                    "trade_pair": trading_pair,
                    "side": trade_side,
                    "size": f'{amount:.8f}',
                    "price": f'{price:.8f}',
                }

                order_result = await self._api_request("post", url, api_params, True)
                exchange_order_id = order_result.get("id")
                self.start_tracking_order(client_order_id,
                                          exchange_order_id,
                                          trading_pair,
                                          trade_type,
                                          price,
                                          amount,
                                          order_type
                                          )
                tracked_order = self._in_flight_orders.get(client_order_id)
                if DEBUG:
                    self.logger().info(f"Created {order_type.name} {trade_type.name} order {client_order_id} for "
                                       f"{amount} {trading_pair}.")
                tracked_order.update_exchange_order_id(exchange_order_id)
                event_tag = MarketEvent.BuyOrderCreated if trade_type is TradeType.BUY else MarketEvent.SellOrderCreated
                event_class = BuyOrderCreatedEvent if trade_type is TradeType.BUY else SellOrderCreatedEvent
                self.trigger_event(event_tag,
                                   event_class(
                                       self.current_timestamp,
                                       order_type,
                                       trading_pair,
                                       amount,
                                       price,
                                       client_order_id,
                                       exchange_order_id))
            except asyncio.CancelledError as e:
                if DEBUG:
                    self.logger().exception("_create_order received a CancelledError...")
                raise e
            except Exception as e:
                if DEBUG:
                    self.logger().exception(f"_create_order received an exception {e}. Details: ")
                self.logger().network(
                    f"Error submitting {trade_type.name} {order_type.name} order to Zebpay for "
                    f"{amount} {trading_pair} "
                    f"{price}.",
                    exc_info=True,
                    app_warning_msg=str(e)
                )
                self.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(
                    self.current_timestamp, client_order_id, order_type))
                self.stop_tracking_order(client_order_id)

    def start_tracking_order(self,
                             order_id: str,
                             exchange_order_id: str,
                             trading_pair: str,
                             trade_type: TradeType,
                             price: Decimal,
                             amount: Decimal,
                             order_type: OrderType):
        """
        Starts tracking an order by simply adding it into _in_flight_orders dictionary.
        """
        if DEBUG:
            if order_id in self._in_flight_orders:
                self.logger().warning(
                    f'start_tracking_order: About to overwrite an in flight order with client_order_id={order_id}'
                )
        self._in_flight_orders[order_id] = ZebpayInFlightOrder(
            client_order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount
        )

    def stop_tracking_order(self, order_id: str):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]
        else:
            if DEBUG:
                self.logger().warning(
                    f'stop_tracking_order: cannot delete order not stored in flight client_order_id={order_id}')

    def get_order_book(self, trading_pair: str) -> OrderBook:
        if trading_pair not in self._order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self._order_book_tracker.order_books[trading_pair]

    async def _status_polling_loop(self):
        """Periodically update user balances and order status via REST API. Fallback measure for ws API updates."""

        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()
                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                    self._update_exchange_info(),
                    self._update_market_info()
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().exception(f'_status_polling_loop received exception: {e}. Details: ')
                self.logger().network("Unexpected error while fetching account updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Zebpay. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)
            finally:
                self._last_poll_timestamp = self.current_timestamp

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN) -> TradeFee:
        # Assumption made here that some limit orders may end up incurring gas fees as takers if they cross the spread.
        # This makes the fee estimate relatively conservative
        is_maker = order_type is OrderType.LIMIT_MAKER
        percent_fees: Decimal = estimate_fee(EXCHANGE_NAME, is_maker).percent
        if is_maker:
            return TradeFee(percent=percent_fees)
        # for taker zebpay v1 collects additional gas fee, collected in the asset received by the taker
        flat_fees = []
        blockchain = get_zebpay_blockchain()  # either ETH or BSC
        gas_limit = ETH_GAS_LIMIT if blockchain == 'ETH' else BSC_GAS_LIMIT
        if HUMMINGBOT_GAS_LOOKUP:
            # resolve gas price from hummingbot's eth_gas_station_lookup
            # conf to be ON for hummingbot to resolve gas price: global_config_map["ethgasstation_gas_enabled"]
            gas_amount: Decimal = eth_gas_station_lookup.get_gas_price(in_gwei=False) * Decimal(gas_limit)
            flat_fees = [(blockchain, gas_amount)]
        elif self._exchange_info and 'gasPrice' in self._exchange_info:
            # or resolve gas price from zebpay exchange endpoint
            gas_price: Decimal = Decimal(self._exchange_info['gasPrice']) / Decimal("1e9")
            gas_amount: Decimal = gas_price * Decimal(gas_limit)
            flat_fees = [(blockchain, gas_amount)]
        return TradeFee(percent=percent_fees, flat_fees=flat_fees)

    async def _update_order_status(self):
        """
        Calls REST API to get status update for each in-flight order.
        """
        last_tick = int(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        current_tick = int(self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            async with self._order_lock:
                if DEBUG:
                    self.logger().warning('entering _update_order_status execution')
                tracked_orders = list(self._in_flight_orders.values())
                if DEBUG:
                    exchange_order_ids = [tracked_order.exchange_order_id for tracked_order in tracked_orders]
                    self.logger().warning(f"Polling order status updates for orders: {exchange_order_ids}")
                tasks = [self.get_order(tracked_order.exchange_order_id) for tracked_order in tracked_orders]
                update_results = await safe_gather(*tasks, return_exceptions=True)
                tracked_order_result = [(o, r) for o, r in zip(tracked_orders, update_results)]
                for tracked_order, result in tracked_order_result:
                    if isinstance(result, Exception):
                        self.logger().error(f"exception in _update_order_status get_order subtask: {result}")
                        # remove failed order from tracked_orders
                        self.stop_tracking_order(tracked_order.client_order_id)
                        self.logger().error(f'Stopped tracking not found order: {tracked_order.client_order_id}')
                        self.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(
                            self.current_timestamp, tracked_order.client_order_id, tracked_order.order_type))
                        continue
                    if DEBUG:
                        self.logger().warning(
                            '_update_order_status is about to call _process_fill_message and _process_order_message '
                            f'for get_order() response: {result}')
                    await self._process_fill_message(result)
                    self._process_order_message(result)

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancellation or failure event if needed.
        :param order_msg: The order response from either REST or web socket API (they are different formats)
        """
        # self.logger().info(f"Order Message: {order_msg}")
        client_order_id = order_msg["c"] if "c" in order_msg else order_msg.get("clientOrderId")
        if client_order_id not in self._in_flight_orders:
            return
        tracked_order = self._in_flight_orders[client_order_id]
        # Update order execution status
        tracked_order.last_state = order_msg["X"] if "X" in order_msg else order_msg.get("status")
        # self.logger().info(f"Tracked Order Status: {tracked_order.last_state}")
        if tracked_order.is_cancelled:
            self.trigger_event(MarketEvent.OrderCancelled,
                               OrderCancelledEvent(
                                   self.current_timestamp,
                                   client_order_id,
                                   tracked_order.exchange_order_id))
            tracked_order.cancelled_event.set()
            self.logger().info(f"The order {client_order_id} is no longer tracked!")
            self.stop_tracking_order(client_order_id)
        elif tracked_order.is_failure:
            self.logger().info(f"The market order {client_order_id} has been rejected according to order status API.")
            self.trigger_event(MarketEvent.OrderFailure,
                               MarketOrderFailureEvent(
                                   self.current_timestamp,
                                   client_order_id,
                                   tracked_order.order_type
                               ))
            self.stop_tracking_order(client_order_id)

    async def _process_fill_message(self, update_msg: Dict[str, Any]):
        """
        Updates in-flight order and trigger order filled event for trade message received. Triggers order completed
        event if the total executed amount equals to the specified order amount.
        """

        client_order_id = update_msg["c"] if "c" in update_msg else update_msg.get("clientOrderId")
        tracked_order = self._in_flight_orders.get(client_order_id)
        if not tracked_order:
            return
        # self.logger().info(f'Update Message:{update_msg}')
        if update_msg.get("F") or update_msg.get("fills") is not None:
            for fill_msg in update_msg["F"] if "F" in update_msg else update_msg.get("fills"):
                self.logger().info(f'Fill Message:{fill_msg}')
                updated = tracked_order.update_with_fill_update(fill_msg)
                if not updated:
                    return
                self.trigger_event(
                    MarketEvent.OrderFilled,
                    OrderFilledEvent(
                        self.current_timestamp,
                        tracked_order.client_order_id,
                        tracked_order.trading_pair,
                        tracked_order.trade_type,
                        tracked_order.order_type,
                        Decimal(str(fill_msg["p"] if "p" in fill_msg else fill_msg.get("price"))),
                        Decimal(str(fill_msg["q"] if "q" in fill_msg else fill_msg.get("quantity"))),
                        TradeFee(0.0, [(fill_msg["a"] if "a" in fill_msg else fill_msg.get("feeAsset"),
                                        Decimal(str(fill_msg["f"] if "f" in fill_msg else fill_msg.get("fee"))))]),
                        exchange_trade_id=update_msg["i"] if "i" in update_msg else update_msg.get("orderId")
                    )
                )
        if math.isclose(tracked_order.executed_amount_base, tracked_order.amount, rel_tol=NORMALIZED_PRECISION) or \
                tracked_order.executed_amount_base >= tracked_order.amount:
            tracked_order.last_state = "filled"
            self.logger().info(f"The {tracked_order.trade_type.name} order "
                               f"{tracked_order.client_order_id} has completed "
                               f"according to order status API.")
            event_tag = MarketEvent.BuyOrderCompleted if tracked_order.trade_type is TradeType.BUY \
                else MarketEvent.SellOrderCompleted
            event_class = BuyOrderCompletedEvent if tracked_order.trade_type is TradeType.BUY \
                else SellOrderCompletedEvent
            self.trigger_event(event_tag,
                               event_class(self.current_timestamp,
                                           tracked_order.client_order_id,
                                           tracked_order.base_asset,
                                           tracked_order.quote_asset,
                                           tracked_order.fee_asset,
                                           tracked_order.executed_amount_base,
                                           tracked_order.executed_amount_quote,
                                           tracked_order.fee_paid,
                                           tracked_order.order_type,
                                           tracked_order.exchange_order_id))
            self.stop_tracking_order(tracked_order.client_order_id)

    async def cancel_all(self, timeout_seconds: float):
        """
        Cancels all in-flight orders and waits for cancellation results.
        Used by bot's top level stop and exit commands (cancelling outstanding orders on exit)
        :param timeout_seconds: The timeout at which the operation will be canceled.
        :returns List of CancellationResult which indicates whether each order is successfully cancelled.
        """
        async with self._order_lock:
            if DEBUG:
                self.logger().warning('<<<< entering cancel_all')
            incomplete_orders = [o for o in self._in_flight_orders.values() if not o.is_done]
            tasks = [self.delete_order(o.exchange_order_id) for o in incomplete_orders]
            order_id_set = set([o.client_order_id for o in incomplete_orders])
            successful_cancellations = []
            try:
                async with timeout(timeout_seconds):
                    results = await safe_gather(*tasks, return_exceptions=True)
                    incomplete_order_result = list(zip(incomplete_orders, results))
                    for incomplete_order, result in incomplete_order_result:
                        if isinstance(result, Exception):
                            self.logger().error(
                                f"exception in cancel_all , subtask delete_order. "
                                f"client_order_id: {incomplete_order.client_order_id}, error: {result}",
                            )
                            continue
                        order_id_set.remove(incomplete_order.client_order_id)
                        successful_cancellations.append(CancellationResult(incomplete_order.client_order_id, True))
                        # todo alf: should we emit event here ?
                        if not result:
                            self.logger().error(
                                f'cancel_all: self.delete_order({incomplete_order.trading_pair}, '
                                f'{incomplete_order.client_order_id}) returned empty response: order not found')
                            response_order_id = '--no-value--'
                        else:
                            response_order_id = (result[0] or {}).get("orderId")
                        if incomplete_order.exchange_order_id != response_order_id:
                            self.logger().error(
                                f"cancel_all: delete_order({incomplete_order.client_order_id}) "
                                f"tracked with exchange id: {incomplete_order.exchange_order_id} "
                                f"returned a different order id {response_order_id}: order not found")
                        # let's stop tracking the order whether we failed or not
                        self.stop_tracking_order(incomplete_order.client_order_id)
                        self.trigger_event(MarketEvent.OrderCancelled,
                                           OrderCancelledEvent(
                                               self.current_timestamp,
                                               incomplete_order.client_order_id,
                                               incomplete_order.exchange_order_id))
                        incomplete_order.cancelled_event.set()
                        self.logger().info(
                            f"cancel_all: finished processing cancel of order:{incomplete_order.client_order_id}. "
                            f"exchange id:{incomplete_order.exchange_order_id}")
            except asyncio.CancelledError as e:
                if DEBUG:
                    self.logger().exception(f"cancel_all got async Cancellation error {e}. Details: ")
                raise e
            except Exception as e:
                if DEBUG:
                    self.logger().exception(f"cancel_all got unexpected Exception error {e}. Details: ")
                self.logger().network(
                    f"Unexpected error cancelling orders. Error: {str(e)}",
                    exc_info=True,
                    app_warning_msg="Failed to cancel order on Zebpay. Check API key and network connection."
                )
            failed_cancellations = [CancellationResult(oid, False) for oid in order_id_set]
            return successful_cancellations + failed_cancellations

    def tick(self, timestamp: float):
        """
        Is called automatically by the clock for each clock's tick (1 second by default).
        It checks if status polling task is due for execution.
        """
        now = time.time()
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if now - self._user_stream_tracker.last_recv_time > 60.0
                         else self.LONG_POLL_INTERVAL)
        last_tick = self._last_timestamp / poll_interval
        current_tick = timestamp / poll_interval
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    async def _update_balances(self, sender=None):
        """ Calls REST API to update total and available balances. """

        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        balance_info = await self.get_balances_from_api()
        for balance in balance_info:
            asset_name = balance["asset"]
            self._account_available_balances[asset_name] = Decimal(str(balance["availableForTrade"]))
            self._account_balances[asset_name] = Decimal(str(balance["quantity"]))
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    @async_ttl_cache(ttl=60 * 10, maxsize=1)
    async def _update_exchange_info(self):
        """Call REST API to update basic exchange info"""
        self._exchange_info = await self.get_exchange_info_from_api()

    @async_ttl_cache(ttl=60 * 10, maxsize=1)
    async def _update_market_info(self):
        """Call REST API to update basic market info"""
        self._market_info = await self.get_market_info_from_api()

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if DEBUG:
                    self.logger().error(f"_iter_user_event_queue Error: {e}")
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Zebpay. Check API key and network connection."
                )
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        """
        Listens to message in _user_stream_tracker.user_stream queue. The messages are put in by
        ZebpayAPIUserStreamDataSource.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                if 'type' not in event_message or 'data' not in event_message:
                    if DEBUG:
                        self.logger().warning(f'unknown event received: {event_message}')
                    continue
                event_type, event_data = event_message['type'], event_message['data']
                if event_type == 'orders':
                    self.logger().info("Receiving WS event")
                    await self._process_fill_message(event_data)
                    self.logger().info(f'event data: {event_data}')
                    self._process_order_message(event_data)
                elif event_type == 'balances':
                    asset_name = event_data['a']
                    # q	quantity	string	Total quantity of the asset held by the wallet on the exchange
                    # f	availableForTrade	string	Quantity of the asset available for trading; quantity - locked
                    # d	usdValue	string	Total value of the asset held by the wallet on the exchange in USD
                    self._account_balances[asset_name] = Decimal(str(event_data['q']))  # todo: q or d ?
                    self._account_available_balances[asset_name] = Decimal(str(event_data['f']))
                elif event_type == 'error':
                    self.logger().error(f"Unexpected error message received from api."
                                        f"Code: {event_data['code']}"
                                        f"message:{event_data['message']}", exc_info=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)