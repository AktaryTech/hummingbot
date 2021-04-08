import itertools
from os.path import join, realpath
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))
import asyncio
import inspect
import unittest
import aiohttp
import logging

from typing import List
from unittest.mock import patch, AsyncMock

from decimal import Decimal

from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from test.integration.assets.mock_data.fixture_zebpay import FixtureZebpay
from hummingbot.connector.exchange.zebpay.zebpay_api_order_book_data_source import ZebpayAPIOrderBookDataSource
from hummingbot.connector.exchange.zebpay.zebpay_order_book_message import ZebpayOrderBookMessage
from hummingbot.connector.exchange.zebpay.zebpay_resolve import get_zebpay_rest_url, get_zebpay_ws_feed
from hummingbot.core.data_type.order_book_message import OrderBookMessageType
from hummingbot.core.data_type.order_book import OrderBook


class ZebpayAPIOrderBookDataSourceUnitTest(unittest.TestCase):

    class AsyncIterator:
        def __init__(self, seq):
            self.iter = iter(seq)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.iter)
            except StopIteration:
                raise StopAsyncIteration

    sample_pairs: List[str] = [
        "DAI-INR",
        "LTC-INR"
    ]

    GET_MOCK: str = 'aiohttp.ClientSession.get'
    REQUEST_MOCK: str = 'requests.get'

    PATCH_BASE_PATH = \
        'hummingbot.connector.exchange.zebpay.zebpay_api_order_book_data_source.ZebpayAPIOrderBookDataSource.{method}'

    @classmethod
    def setUpClass(cls) -> None:
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.order_book_data_source: ZebpayAPIOrderBookDataSource = ZebpayAPIOrderBookDataSource(cls.sample_pairs)

    def run_async(self, task):
        return self.ev_loop.run_until_complete(task)

    # Success
    def test_get_zebpay_rest_url(self):
        self.assertEqual("https://www.zebapi.com/pro/v1", get_zebpay_rest_url())

    # Success
    def test_get_zebpay_ws_feed(self):
        self.assertEqual("wss://ws-feed.zebpay.com/marketdata", get_zebpay_ws_feed())

    # Success
    def test_fetch_trading_pairs(self):
        # ETH URL
        trading_pairs: List[str] = self.run_async(
            self.order_book_data_source.fetch_trading_pairs())
        self.assertIn("DAI-INR", trading_pairs)
        self.assertIn("LTC-INR", trading_pairs)

    # Success
    @patch(GET_MOCK, new_callable=AsyncMock)
    def test_get_last_traded_price(self, mocked_get):
        for t_pair in self.sample_pairs:
            mocked_get.return_value.json.return_value = FixtureZebpay.TRADING_PAIR_TRADES
            mocked_get.return_value.status = 200
            last_traded_price: float = self.run_async(
                self.order_book_data_source.get_last_traded_price(t_pair, "https://www.zebpay.com/pro/v1"))
            self.assertEqual(0.0005, last_traded_price)

    # Success
    @patch(GET_MOCK, new_callable=AsyncMock)
    def test_get_last_traded_prices(self, mocked_get):
        # ETH URL
        mocked_get.return_value.json.return_value = FixtureZebpay.TRADING_PAIR_TRADES
        mocked_get.return_value.status = 200
        last_traded_prices: List[str] = self.run_async(
            self.order_book_data_source.get_last_traded_prices(self.sample_pairs))
        self.assertEqual({"DAI-INR": 0.0005,
                         "LTC-INR": 0.0005}, last_traded_prices)

    # Requires review: receiving error "AttributeError: 'coroutine' object has no attribute 'json'"
    # Likely due to the synchronous nature of requests.get called in get_mid_price. May require different mock format?
    @patch(REQUEST_MOCK, new_callable=AsyncMock)
    def test_get_mid_price(self, mocked_get):
        mocked_get.return_value.json.return_value = FixtureZebpay.TRADING_PAIR_TICKER
        for t_pair in self.sample_pairs:
            t_pair_mid_price: List[str] = self.order_book_data_source.get_mid_price(t_pair)
            # TODO: Confirm mid-price value
            self.assertEqual(Decimal("14963.00"), t_pair_mid_price)
            self.assertIsInstance(t_pair_mid_price, Decimal)

    async def get_snapshot(self, trading_pair):
        async with aiohttp.ClientSession() as client:
            try:
                snapshot = await self.order_book_data_source.get_snapshot(client, trading_pair)
                return snapshot
            except Exception:
                return None

    @patch('aiohttp.ClientResponse.json')
    def test_get_snapshot(self, mocked_json):

        # mocked_get.return_value.json.return_value = FixtureZebpay.ORDER_BOOK_LEVEL2
        # mocked_get.return_value.status = 200

        # Mock aiohttp response
        f = asyncio.Future()
        f.set_result(FixtureZebpay.ORDER_BOOK_LEVEL2)
        mocked_json.return_value = f

        # mocked_get.return_value.__aenter__.return_value.text = AsyncMock(side_effect=["custom text"])
        # mocked_get.return_value.__aexit__.return_value = AsyncMock(side_effect=lambda *args: True)
        # mocked_get.return_value = MockGetResponse(FixtureZebpay.ORDER_BOOK_LEVEL2, 200)

        snapshot = self.ev_loop.run_until_complete(self.get_snapshot("btc-aud"))
        # an artifact created by the way we mock. Normally run_until_complete() returns a result directly
        snapshot = snapshot.result()
        self.assertEqual(FixtureZebpay.ORDER_BOOK_LEVEL2, snapshot)

    @patch(PATCH_BASE_PATH.format(method='get_snapshot'))
    def test_get_new_order_book(self, mock_get_snapshot):

        # Mock Future() object return value as the request response
        # For this particular test, the return value from get_snapshot is not relevant, therefore
        # setting it with a random snapshot from fixture
        f = asyncio.Future()
        f.set_result(FixtureZebpay.SNAPSHOT_2)
        mock_get_snapshot.return_value = f.result()

        orderbook = self.ev_loop.run_until_complete(self.order_book_data_source.get_new_order_book("btc-aud"))

        print(orderbook.snapshot[0])

        # Validate the returned value is OrderBook
        self.assertIsInstance(orderbook, OrderBook)

        # Ensure the number of bids / asks provided in the snapshot are equal to the respective number of orderbook rows
        self.assertEqual(len(orderbook.snapshot[0].index), len(FixtureZebpay.SNAPSHOT_2["bids"]))

    @patch(PATCH_BASE_PATH.format(method='get_snapshot'))
    def test_get_tracking_pairs(self, mock_get_snapshot):

        # Mock Future() object return value as the request response
        # For this particular test, the return value from get_snapshot is not relevant, therefore
        # setting it with a random snapshot from fixture
        f = asyncio.Future()
        f.set_result(FixtureZebpay.SNAPSHOT_2)
        mock_get_snapshot.return_value = f.result()

        tracking_pairs = self.ev_loop.run_until_complete(self.order_book_data_source.get_tracking_pairs())

        # Validate the number of tracking pairs is equal to the number of trading pairs received
        self.assertEqual(len(self.sample_pairs), len(tracking_pairs))

        # Make sure the entry key in tracking pairs matches with what's in the trading pairs
        for trading_pair, tracking_pair_obj in zip(self.sample_pairs, list(tracking_pairs.keys())):
            self.assertEqual(trading_pair, tracking_pair_obj)

        # Validate the data type for each tracking pair value is OrderBookTrackerEntry
        for order_book_tracker_entry in tracking_pairs.values():
            self.assertIsInstance(order_book_tracker_entry, OrderBookTrackerEntry)

        # Validate the order book tracker entry trading_pairs are valid
        for trading_pair, order_book_tracker_entry in zip(self.sample_pairs, tracking_pairs.values()):
            self.assertEqual(order_book_tracker_entry.trading_pair, trading_pair)

    @patch(PATCH_BASE_PATH.format(method='get_snapshot'))
    def test_listen_for_order_book_snapshots(self, mock_get_snapshot, mock_api_url):

        # Instantiate empty async queue and make sure the initial size is 0
        q = asyncio.Queue()
        self.assertEqual(q.qsize(), 0)

        # Mock Future() object return value as the request response
        # For this particular test, the return value from get_snapshot is not relevant, therefore
        # setting it with a random snapshot from fixture
        f1 = asyncio.Future()
        f1.set_result(FixtureZebpay.SNAPSHOT_1)

        # Mock Future() object return value as the request response
        # For this particular test, the return value from get_snapshot is not relevant, therefore
        # setting it with a random snapshot from fixture
        f2 = asyncio.Future()
        f2.set_result(FixtureZebpay.SNAPSHOT_2)

        mock_get_snapshot.side_effect = [f1.result(), f2.result()]

        # Listening for tracking pairs within the set timeout timeframe
        timeout = 6

        print('{test_name} is going to run for {timeout} seconds, starting now'.format(
            test_name=inspect.stack()[0][3],
            timeout=timeout))

        try:
            self.run_async(
                # Force exit from event loop after set timeout seconds
                asyncio.wait_for(
                    self.order_book_data_source.listen_for_order_book_snapshots(ev_loop=self.ev_loop, output=q),
                    timeout=timeout
                )
            )
        except asyncio.exceptions.TimeoutError as e:
            print(e)

        # Make sure that the number of items in the queue after certain seconds make sense
        # For instance, when the asyncio sleep time is set to 5 seconds in the method
        # If we configure timeout to be the same length, only 1 item has enough time to be received
        self.assertGreaterEqual(q.qsize(), 1)

        # Validate received response has correct data types
        first_item = q.get_nowait()
        self.assertIsInstance(first_item, ZebpayOrderBookMessage)
        self.assertIsInstance(first_item.type, OrderBookMessageType)

        # Validate order book message type
        self.assertEqual(first_item.type, OrderBookMessageType.SNAPSHOT)

        # Validate snapshot received matches with the original snapshot received from API
        self.assertEqual(first_item.content['bids'], FixtureZebpay.SNAPSHOT_1['bids'])
        self.assertEqual(first_item.content['asks'], FixtureZebpay.SNAPSHOT_1['asks'])

        # Validate the rest of the content
        self.assertEqual(first_item.content['trading_pair'], self.eth_sample_pairs[0])
        self.assertEqual(first_item.content['sequence'], FixtureZebpay.SNAPSHOT_1['sequence'])

    @patch(PATCH_BASE_PATH.format(method='_inner_messages'))
    def test_listen_for_order_book_diffs(self, mock_inner_messages, mock_ws_feed):
        timeout = 2

        q = asyncio.Queue()

        #  Socket events receiving in the order from top to bottom
        mocked_socket_responses = itertools.cycle(
            [
                FixtureZebpay.WS_PRICE_LEVEL_UPDATE_1,
                FixtureZebpay.WS_PRICE_LEVEL_UPDATE_2,
                FixtureZebpay.WS_SUBSCRIPTION_SUCCESS
            ]
        )

        mock_inner_messages.return_value = self.AsyncIterator(seq=mocked_socket_responses)

        print('{test_name} is going to run for {timeout} seconds, starting now'.format(
            test_name=inspect.stack()[0][3],
            timeout=timeout))

        try:
            self.run_async(
                # Force exit from event loop after set timeout seconds
                asyncio.wait_for(
                    self.order_book_data_source.listen_for_order_book_diffs(ev_loop=self.ev_loop, output=q),
                    timeout=timeout
                )
            )
        except asyncio.exceptions.TimeoutError as e:
            print(e)

        first_event = q.get_nowait()
        second_event = q.get_nowait()

        recv_events = [first_event, second_event]

        for event in recv_events:
            # Validate the data inject into async queue is in Liquid order book message type
            self.assertIsInstance(event, ZebpayOrderBookMessage)

            # Validate the event type is equal to DIFF
            self.assertEqual(event.type, OrderBookMessageType.DIFF)

            # Validate the actual content injected is dict type
            self.assertIsInstance(event.content, dict)

    @patch(PATCH_BASE_PATH.format(method='_inner_messages'))
    def test_listen_for_trades(self, mock_inner_messages):
        timeout = 2

        q = asyncio.Queue()

        #  Socket events receiving in the order from top to bottom
        mocked_socket_responses = itertools.cycle(
            [
                FixtureZebpay.WS_TRADE_1,
                FixtureZebpay.WS_TRADE_2
            ]
        )

        mock_inner_messages.return_value = self.AsyncIterator(seq=mocked_socket_responses)

        print('{test_name} is going to run for {timeout} seconds, starting now'.format(
            test_name=inspect.stack()[0][3],
            timeout=timeout))

        try:
            self.run_async(
                # Force exit from event loop after set timeout seconds
                asyncio.wait_for(
                    self.order_book_data_source.listen_for_trades(ev_loop=self.ev_loop, output=q),
                    timeout=timeout
                )
            )
        except asyncio.exceptions.TimeoutError as e:
            print(e)

        first_event = q.get_nowait()
        second_event = q.get_nowait()

        recv_events = [first_event, second_event]

        for event in recv_events:
            # Validate the data inject into async queue is in Liquid order book message type
            self.assertIsInstance(event, ZebpayOrderBookMessage)

            # Validate the event type is equal to DIFF
            self.assertEqual(event.type, OrderBookMessageType.TRADE)

            # Validate the actual content injected is dict type
            self.assertIsInstance(event.content, dict)


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
