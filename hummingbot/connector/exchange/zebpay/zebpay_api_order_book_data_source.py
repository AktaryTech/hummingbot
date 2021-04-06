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

from hummingbot.connector.exchange.zebpay.zebpay_active_order_tracker import zebpayActiveOrderTracker
from hummingbot.connector.exchange.zebpay.zebpay_order_book_tracker_entry import zebpayOrderBookTrackerEntry
from hummingbot.connector.exchange.zebpay.zebpay_order_book import zebpayOrderBook
from hummingbot.connector.exchange.zebpay.zebpay_resolve import get_zebpay_rest_url, get_zebpay_ws_feed, get_throttler
from hummingbot.connector.exchange.zebpay.zebpay_utils import DEBUG

MAX_RETRIES = 20
NaN = float("nan")


class zebpayAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _zebpay_REST_URL: str = None
    _zebpay_WS_FEED: str = None

    _iaobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._iaobds_logger is None:
            cls._iaobds_logger = logging.getLogger(__name__)
        return cls._iaobds_logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(trading_pairs)

    # Found last trading price in Zebpay API. Utilized safe_gather to complete all tasks and append last trade prices
    # for all trading pairs on results list.
    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str], domain=None) -> Dict[str, float]:
        base_url: str = get_zebpay_rest_url(domain=domain)
        tasks = [cls.get_last_traded_price(t_pair, base_url) for t_pair in trading_pairs]
        results = await safe_gather(*tasks)
        return {t_pair: result for t_pair, result in zip(trading_pairs, results)}

    @classmethod
    async def get_last_traded_price(cls, trading_pair: str, base_url: str = "") -> float:   # update str w/ zebpay url
        async with get_throttler().weighted_task(request_weight=1):
            async with aiohttp.ClientSession() as client:
                url = f"{base_url}/v1/trades/?market={trading_pair}"                        # update url w/ zebpay url
                resp = await client.get(url)
                if resp.status != 200:
                    data = await resp.json()
                    raise IOError(f"Error fetching data from {url}. HTTP status is {resp.status}."
                                  f"Data is: {data}")
                # based on previous GET requests to the Zebpay trade URL, the most recent trade is located at the -1
                # index of the returned list of trades. This assumes pop() on the returned list is the optimal solution
                # for retrieving the latest trade. Confirm this is also true for Zebpay.
                try:
                    resp_json = await resp.json()
                    last_trade = resp_json[-1]
                    return float(last_trade["price"])
                except Exception as e:
                    raise IOError(f"Error fetching data from {url}. resp_json: {resp_json}."
                                  f"exception: {e}")

    @classmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(cls, trading_pair: str, domain=None) -> Optional[Decimal]:
        base_url: str = get_zebpay_rest_url(domain=domain)
        ticker_url: str = f"{base_url}/v1/tickers?market={trading_pair}"        # change to Zebpay url
        resp = requests.get(ticker_url)
        market = resp.json()
        if market.get('bid') and market.get('ask'):
            result = (Decimal(market['bid']) + Decimal(market['ask'])) / Decimal('2')
            return result

    @staticmethod
    async def fetch_trading_pairs(domain=None) -> List[str]:
        async with get_throttler().weighted_task(request_weight=1):
            try:
                async with aiohttp.ClientSession() as client:
                    base_url: str = get_zebpay_rest_url(domain=domain)
                    async with client.get(f"{base_url}/v1/tickers", timeout=5) as response:
                        if response.status == 200:
                            markets = await response.json()
                            raw_trading_pairs: List[str] = list(map(lambda details: details.get('market'), markets))
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
        :returns: Response from the rest API
        """
        async with get_throttler().weighted_task(request_weight=1):
            # Zebpay level 2 order book is sufficient to provide required data. Confirm this
            base_url: str = get_zebpay_rest_url()
            product_order_book_url: str = f"{base_url}/v1/orderbook?market={trading_pair}&level=2" # adjust to zebpay ur
            async with client.get(product_order_book_url) as response:
                response: aiohttp.ClientResponse = response
                if response.status != 200:
                    raise IOError(f"Error fetching Zebpay market snapshot for {trading_pair}."
                                  f"HTTP status is {response.status}.")
                data: Dict[str, Any] = await response.json()
                return data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = zebpayOrderBook.snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair}
            )
            active_order_tracker: zebpayActiveOrderTracker = zebpayActiveOrderTracker()
            bids, asks = active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
            order_book = self.order_book_create_function()
            order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
            return order_book

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        """
        *required
        Initializes order books and order book trackers for the list of trading pairs
        returned by `self.fetch_trading_pairs`
        :returns: A dictionary of order book trackers for each trading pair
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
                    snapshot_msg: OrderBookMessage = zebpayOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"trading_pair": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    active_order_tracker: zebpayActiveOrderTracker = zebpayActiveOrderTracker()
                    bids, asks = active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
                    order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)

                    retval[trading_pair] = zebpayOrderBookTrackerEntry(
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

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        """
        Generator function that returns messages from the web socket stream
        :param ws: current web socket connection
        :returns: message in AsyncIterable format
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
            self.logger().warning("Websock ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        *required
        Subscribe to trade channel via Zebpay WebSocket and keep the connection open for incoming messages.
        WebSocket trade subscription response example:
            Input here...
        :param ev_loop: ev_loop to execute this function in
        :param output: an async queue where the incoming messages are stored
        """

        while True:
            zebpay_ws_feed = get_zebpay_ws_feed()
            if DEBUG:
                self.logger().info(f"IOB.listen_for_trades new connection to ws: {zebpay_ws_feed}")
            try:
                trading_pairs: List[str] = self._trading_pairs
                async with websockets.connect(zebpay_ws_feed) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    subscription_request: Dict[str, Any] = {
                        "method": "subscribe",
                        "markets": trading_pairs,
                        "subscriptions": ["trades"]
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
                            trade_timestamp: float = pd.Timestamp(msg["data"]["t"], unit="ms").timestamp()
                            trade_msg: OrderBookMessage = zebpayOrderBook.trade_message_from_exchange(msg,
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
        *required
        Subscribe to  channel via web socket, and keep the connection open for incoming messages
        WebSocket trade subscription response example:
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
                        "method": "subscribe",
                        "markets": trading_pairs,
                        "subscriptions": ["l2orderbook"]
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
                            diff_timestamp: float = pd.Timestamp(msg["data"]["t"], unit="ms").timestamp()
                            order_book_message: OrderBookMessage = \
                                zebpayOrderBook.diff_message_from_exchange(msg, diff_timestamp)
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
        while True:
            try:
                async with aiohttp.ClientSession() as client:
                    for trading_pair in self._trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            if DEBUG:
                                self.logger().info(f'<<<<< aiohttp snapshot response: {snapshot}')
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = zebpayOrderBook.snapshot_message_from_exchange(
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