import asyncio
import aiohttp
import logging

import cachetools.func
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
)
from decimal import Decimal
import time

import requests
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource

# import with change to get_last_traded_prices
from hummingbot.core.utils.async_utils import safe_gather

from hummingbot.connector.exchange.zebpay.zebpay_active_order_tracker import ZebpayActiveOrderTracker
from hummingbot.connector.exchange.zebpay.zebpay_order_book_tracker_entry import ZebpayOrderBookTrackerEntry
from hummingbot.connector.exchange.zebpay.zebpay_order_book import ZebpayOrderBook
from hummingbot.connector.exchange.zebpay.zebpay_resolve import get_zebpay_rest_url, get_zebpay_ws_feed, get_throttler
from hummingbot.connector.exchange.zebpay.zebpay_utils import DEBUG

MAX_RETRIES = 20
NaN = float("nan")


class ZebpayAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _zebpay_REST_URL: str = None
    _zebpay_WS_FEED: str = None

    _zaobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._zaobds_logger is None:
            cls._zaobds_logger = logging.getLogger(__name__)
        return cls._zaobds_logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(trading_pairs)

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str], domain=None) -> Dict[str, float]:
        """
        Returns a dictionary containing the last traded price for each trading pair.
        :param trading_pairs: A list of trading pairs supported by the Zebpay exchange
        :param domain: The domain used in the method call (Zebpay Exchange or Zebpay Sandbox)
        :returns a dictionary of trading pairs and associated last traded prices
        """
        base_url: str = get_zebpay_rest_url(domain=domain)
        tasks = [cls.get_last_traded_price(t_pair, base_url) for t_pair in trading_pairs]
        results = await safe_gather(*tasks)
        return {t_pair: result for t_pair, result in zip(trading_pairs, results)}

    @classmethod
    async def get_last_traded_price(cls, trading_pair: str, base_url: str = "https://www.zebapi.com/pro/v1") -> float:
        """
        Returns a float of the last traded price for the given trading pair.
        :param trading_pair: The trading pair used in the price query
        :param base_url: The url used in the GET request (Zebpay Exchange or Zebpay Sandbox)
        :returns a float of the last fill price for the given trading pair
        """
        async with get_throttler().weighted_task(request_weight=1):
            async with aiohttp.ClientSession() as client:
                url = f"{base_url}/market/{trading_pair}/trades"
                resp = await client.get(url)
                if resp.status != 200:
                    data = await resp.json()
                    raise IOError(f"Error fetching data from {url}. HTTP status is {resp.status}."
                                  f"Data is: {data}")
                try:
                    resp_json = await resp.json()
                    last_trade = resp_json[0]
                    return float(last_trade["fill_price"])
                except Exception as e:
                    raise IOError(f"Error fetching data from {url}. resp_json: {resp_json}."
                                  f"exception: {e}")

    @classmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(cls, trading_pair: str, domain=None) -> Optional[Decimal]:
        """
        Returns a Decimal representing the mid-price between the current bid and ask prices.
        :param trading_pair: The trading pair used in the price query
        :param domain: The domain used in the method call (Zebpay Exchange or Zebpay Sandbox)
        :returns A Decimal representing the mid-price between the current bid and ask prices.
        """
        base_url: str = get_zebpay_rest_url(domain=domain)
        ticker_url: str = f"{base_url}/market/{trading_pair}/ticker"
        resp = requests.get(ticker_url)
        market = resp.json()
        if market.get('buy') and market.get('sell'):
            result = (Decimal(market['buy']) + Decimal(market['sell'])) / Decimal('2')
            return result

    @staticmethod
    # TODO Brian: Coordinate with Zebpay on API 404 error. All trading pairs that have a string value in the "volume"
    #  field return a 404 Not Found error when queried by any public endpoint requests.
    async def fetch_trading_pairs(domain=None) -> List[str]:
        """
        Returns a list of all trading pairs available on the Zebpay exchange domain.
        :param domain: The domain used in the method call (Zebpay Exchange or Zebpay Sandbox)
        :returns a list of of all trading pairs available on the Zebpay exchange domain.
        """
        async with get_throttler().weighted_task(request_weight=1):
            try:
                async with aiohttp.ClientSession() as client:
                    base_url: str = get_zebpay_rest_url(domain=domain)
                    async with client.get(f"{base_url}/market", timeout=5) as response:
                        if response.status == 200:
                            markets = await response.json()
                            raw_trading_pairs: List[str] = list(map(lambda details: details.get('pair'), markets))
                            trading_pair_list: List[str] = []
                            for raw_trading_pair in raw_trading_pairs:
                                trading_pair_list.append(raw_trading_pair)
                            return trading_pair_list

            except Exception:
                # Do nothing if request fails. No autocomplete for trading pairs.
                pass

            return []

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        """
        Fetches order book snapshot for a particular trading pair from the rest API
        :param client: The client session used to make the API request
        :param trading_pair: The trading pair used in the snapshot query
        :returns an orderbook snapshot of bids/asks
        """
        async with get_throttler().weighted_task(request_weight=1):
            # Zebpay Orderbook API provides the first 15 bids and asks on the orderbook
            # Todo Brian: Zebpay orderbook does not provide a sequence number - will rely on timestamp for sequencing
            base_url: str = get_zebpay_rest_url()
            product_order_book_url: str = f"{base_url}/market/{trading_pair}/book"
            async with client.get(product_order_book_url) as response:
                response: aiohttp.ClientResponse = response
                if response.status != 200:
                    raise IOError(f"Error fetching Zebpay market snapshot for {trading_pair}."
                                  f"HTTP status is {response.status}.")
                data: Dict[str, Any] = await response.json()
                return data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        """
        Creates formatted orderbook instance for given trading pair
        :param trading_pair: The trading pair used in the snapshot query
        :returns an orderbook instance of the given trading pair
        """
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = ZebpayOrderBook.snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair}
            )
            active_order_tracker: ZebpayActiveOrderTracker = ZebpayActiveOrderTracker()
            bids, asks = active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
            order_book = self.order_book_create_function()
            order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
            return order_book

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        """
        Initializes orderbooks and orderbook trackers for the list of trading pairs returned by fetch_trading_pairs
        :returns a dictionary of orderbooks for each trading pair
        """
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = self._trading_pairs
            retval: Dict[str, OrderBookTrackerEntry] = {}
            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: OrderBookMessage = ZebpayOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"trading_pair": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    active_order_tracker: ZebpayActiveOrderTracker = ZebpayActiveOrderTracker()
                    bids, asks = active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
                    order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)

                    retval[trading_pair] = ZebpayOrderBookTrackerEntry(
                        trading_pair,
                        snapshot_timestamp,
                        order_book,
                        active_order_tracker
                    )
                    self.logger().info(f"Initialized order book for {trading_pair}."
                                       f"{index + 1}/{number_of_pairs} completed.")
                    await asyncio.sleep(0.6)
                except IOError:
                    self.logger().network(
                        f"Error getting snapshot for {trading_pair}.",
                        exc_info=True,
                        app_warning_msg=f"Error getting snapshot for {trading_pair}. Check network connection."
                    )
                except Exception:
                    self.logger().error(f"Error initializing order book for {trading_pair}.", exc_info=True)
            return retval

    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        """
        Generator function that returns messages from the websocket stream
        :param ws: the websocket client protocol used to connect to the Zebpay websocket API
        :returns message from Zebpay websocket stream
        """
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("Websocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Subscribe to "history" channel via Zebpay WebSocket and keep the connection open for incoming messages.
        WebSocket history subscription response example:
            Input example here...
        :param ev_loop: ev_loop to execute this function in
        :param output: an async queue where the incoming messages are stored
        """

        while True:
            zebpay_ws_feed = get_zebpay_ws_feed()
            if DEBUG:
                self.logger().info(f"ZOB.listen_for_trades new connection to ws: {zebpay_ws_feed}")
            try:
                trading_pairs: List[str] = self._trading_pairs
                async with websockets.connect(zebpay_ws_feed) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    subscription_request: Dict[str, Any] = {
                        # TODO Brian: Review subscription request format for Zebpay
                        "method": "subscribe",
                        "markets": trading_pairs,
                        # Working assumption that Zebpay WS trades are in the "history" subscription. Require format
                        "subscriptions": ["history"]
                    }
                    await ws.send(ujson.dumps(subscription_request))
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        msg_type: str = msg.get("type", None)
                        if DEBUG:
                            self.logger().debug(f'<<<<< ws msg: {msg}')
                        if msg_type is None:
                            raise ValueError(f"Zebpay Websocket message does not contain a type - {msg}")
                        elif msg_type == "error":
                            raise ValueError(f"Zebpay Websocket received error message - {msg['data']['message']}")
                        elif msg_type == "trades":
                            # TODO Brian: Confirm lastModifiedDate key parsing is correct
                            trade_timestamp: float = pd.Timestamp(msg["lastModifiedDate"], unit="ms").timestamp()
                            trade_msg: OrderBookMessage = ZebpayOrderBook.trade_message_from_exchange(msg,
                                                                                                      trade_timestamp)
                            output.put_nowait(trade_msg)
                        elif msg_type == "subscriptions":
                            self.logger().info("subscription to trade received")
                        else:
                            raise ValueError(f"Unrecognized Zebpay WebSocket message received - {msg}")
                        await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f'{"Unexpected error with WebSocket connection."}',
                    exc_info=True,
                    app_warning_msg=f'{"Unexpected error with Websocket connection. Retrying in 30 seconds..."}'
                                    f'{"Check network connection."}'
                )
                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Subscribe to "book" channel via Zebpay WebSocket and keep the connection open for incoming messages.
        WebSocket book subscription response example:
            Input example here...
        :param ev_loop: ev_loop to execute this function in
        :param output: an async queue where the incoming messages are stored
        """

        while True:
            zebpay_ws_feed = get_zebpay_ws_feed()
            if DEBUG:
                self.logger().info(f"IOB.listen_for_order_book_diffs new connection to ws: {zebpay_ws_feed}")
            try:
                trading_pairs: List[str] = self._trading_pairs
                async with websockets.connect(zebpay_ws_feed) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    subscription_request: Dict[str, Any] = {
                        # TODO Brian: Review subscription request format for Zebpay
                        "method": "subscribe",
                        "markets": trading_pairs,
                        # Working assumption that Zebpay WS diffs are in the "book" subscription. Require format
                        "subscriptions": ["book"]
                    }
                    await ws.send(ujson.dumps(subscription_request))
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        msg_type: str = msg.get("type", None)
                        if DEBUG:
                            self.logger().debug(f'<<<<< ws msg: {msg}')
                        if msg_type is None:
                            raise ValueError(f"Zebpay WebSocket message does not contain a type - {msg}")
                        elif msg_type == "error":
                            raise ValueError(f"Zebpay WebSocket message received error message - "
                                             f"{msg['data']['message']}")
                        elif msg_type == "l2orderbook":
                            # TODO Brian: Confirm whether WS orderbook diffs have a timestamp
                            diff_timestamp: float = pd.Timestamp(msg["data"]["t"], unit="ms").timestamp()
                            order_book_message: OrderBookMessage = \
                                ZebpayOrderBook.diff_message_from_exchange(msg, diff_timestamp)
                            output.put_nowait(order_book_message)
                        elif msg_type == "subscriptions":
                            self.logger().info("subscription to l2orderbook received")
                        else:
                            raise ValueError(f"Unrecognized Zebpay WebSocket message received - {msg}")
                        await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f'{"Unexpected error with WebSocket connection."}',
                    exc_info=True,
                    app_warning_msg=f'{"Unexpected error with WebSocket connection. Retrying in 30 seconds."}'
                                    f'{"Check network connection."}'
                )
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listens for orderbooks snapshots returned by the Zebpay API and updates respective trading pair orderbook
        :param ev_loop: ev_loop to execute this function in
        :param output: an async queue where the incoming messages are stored
        """
        while True:
            try:
                async with aiohttp.ClientSession() as client:
                    for trading_pair in self._trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            if DEBUG:
                                self.logger().info(f'<<<<< aiohttp snapshot response: {snapshot}')
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = ZebpayOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            if DEBUG:
                                self.logger().info(f"Saved orderbook snapshot for {trading_pair}")
                            # Be careful not to go above API rate limits
                            await asyncio.sleep(0.2)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)