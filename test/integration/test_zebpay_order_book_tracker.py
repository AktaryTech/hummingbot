#!/usr/bin/env python
import datetime
import math
from os.path import join, realpath
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))

from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import OrderBookEvent, OrderBookTradeEvent, TradeType
from hummingbot.connector.exchange.zebpay.zebpay_order_book_tracker import ZebpayOrderBookTracker
from hummingbot.connector.exchange.zebpay.zebpay_api_order_book_data_source import ZebpayAPIOrderBookDataSource
import asyncio
import logging
from typing import (
    Dict,
    Optional,
    List,
)
import unittest

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)

from hummingbot.logger import struct_logger
import hummingbot.connector.exchange.zebpay.zebpay_resolve


# force resolution of api base url for conf values provided to this test
hummingbot.connector.exchange.zebpay.zebpay_resolve._IS_ZEBPAY_SANDBOX = True


# Set log level for this test
# LOG_LEVEL = logging.DEBUG
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(levelname)s - %(filename)s:%(lineno)s - %(funcName)s(): %(message)s"


_logger = None


def logger():
    global _logger

    def setup_logging():
        logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
        logging.addLevelName(struct_logger.EVENT_LOG_LEVEL, "EVENT_LOG")
        logging.addLevelName(struct_logger.METRICS_LOG_LEVEL, "METRIC_LOG")
        logging.getLogger("hummingbot.core.event.event_reporter").setLevel(logging.DEBUG)

    if _logger is None:
        setup_logging()
        _logger = logging.getLogger(__name__)
    return _logger


class ZebpayOrderBookTrackerUnitTest(unittest.TestCase):

    order_book_tracker: Optional[ZebpayOrderBookTracker] = None
    events: List[OrderBookEvent] = [
        OrderBookEvent.TradeEvent
    ]

    sample_pairs: List[str] = [
        "DAI-INR",
        "LTC-INR"
    ]

    logger = None

    @classmethod
    def setUpClass(cls):
        cls.logger = logger()
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.ev_loop.set_debug(True)
        cls.order_book_tracker: ZebpayOrderBookTracker = ZebpayOrderBookTracker(trading_pairs=cls.sample_pairs)

        cls.order_book_tracker_task: asyncio.Task = safe_ensure_future(cls.order_book_tracker.start())
        cls.ev_loop.run_until_complete(
            asyncio.wait_for(
                cls.wait_til_tracker_ready(),
                timeout=5 * 60  # timeout to force tests termination on failure
            )
        )

    @classmethod
    async def wait_til_tracker_ready(cls):
        while True:
            if len(cls.order_book_tracker.order_books) > 0:
                print("Initialized real-time order books.")
                return
            await asyncio.sleep(1)

    async def run_parallel_async(self, *tasks):
        # safe_gather takes the coroutines (*tasks) and returns their futures in a list
        # safe_ensure_future receives those futures without requiring await and returns those futures
        future: asyncio.Future = safe_ensure_future(safe_gather(*tasks))
        while not future.done():
            await asyncio.sleep(1.0)
        return future.result()

    def run_parallel(self, *tasks):
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks))

    def setUp(self):
        self.event_logger = EventLogger()
        for event_tag in self.events:
            for trading_pair, order_book in self.order_book_tracker.order_books.items():
                order_book.add_listener(event_tag, self.event_logger)

    def test_order_book_trade_event_emission(self):
        """
        Test if order book tracker is able to retrieve order book trade message from exchange and
        emit order book trade events after correctly parsing the trade messages
        """
        self.run_parallel(self.event_logger.wait_for(OrderBookTradeEvent))
        for ob_trade_event in self.event_logger.event_log:
            print(f"ob_trade_event: {ob_trade_event}")
            self.assertTrue(type(ob_trade_event) == OrderBookTradeEvent)
            self.assertTrue(ob_trade_event.trading_pair in self.sample_pairs)
            self.assertTrue(type(ob_trade_event.timestamp) == float)
            self.assertTrue(type(ob_trade_event.amount) == float)
            self.assertTrue(type(ob_trade_event.price) == float)
            self.assertTrue(type(ob_trade_event.type) == TradeType)
            self.assertTrue(datetime.datetime.fromtimestamp(ob_trade_event.timestamp).year >= 2021)
            self.assertTrue(ob_trade_event.amount > 0)
            self.assertTrue(ob_trade_event.price > 0)

    def test_tracker_integrity(self):
        # Wait 5 seconds to process some diffs.
        self.ev_loop.run_until_complete(asyncio.sleep(10.0))
        order_books: Dict[str, OrderBook] = self.order_book_tracker.order_books
        dil_eth_book: OrderBook = order_books["DAI-INR"]
        pip_eth_book: OrderBook = order_books["LTC-INR"]
        self.assertGreaterEqual(dil_eth_book.get_price_for_volume(True, 1).result_price,
                                dil_eth_book.get_price(True))
        self.assertLessEqual(dil_eth_book.get_price_for_volume(False, 1).result_price,
                             dil_eth_book.get_price(False))
        self.assertGreaterEqual(pip_eth_book.get_price_for_volume(True, 3).result_price,
                                pip_eth_book.get_price(True))
        self.assertLessEqual(pip_eth_book.get_price_for_volume(False, 3).result_price,
                             pip_eth_book.get_price(False))
        for order_book in self.order_book_tracker.order_books.values():
            print(order_book.last_trade_price)
            self.assertFalse(math.isnan(order_book.last_trade_price))

    def test_api_get_last_traded_prices(self):
        zebpay_ob_data_source = ZebpayAPIOrderBookDataSource(["DAI-INR", "LTC-INR"])
        prices = self.ev_loop.run_until_complete(zebpay_ob_data_source.get_last_traded_prices(["DAI-INR", "LTC-INR"]))
        for key, value in prices.items():
            print(f"{key} last_trade_price: {value}")
        self.assertGreater(prices["DAI-INR"], 0.07)
        self.assertLess(prices["LTC-INR"], 0.06)


def main():
    unittest.main()


if __name__ == "__main__":
    main()