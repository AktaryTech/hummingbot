#!/usr/bin/env python

import logging

from sqlalchemy.engine import RowProxy
from typing import (
    Optional,
    Dict,
    List, Any)
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage, OrderBookMessageType
)
from hummingbot.connector.exchange.zebpay.zebpay_order_book_message import ZebpayOrderBookMessage

_zob_logger = None


class ZebpayOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _zob_logger
        if _zob_logger is None:
            _zob_logger = logging.getLogger(__name__)
        return _zob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, Any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        *required
        Convert JSON snapshot data into standard OrderBookMessage format
        :param msg: JSON snapshot data from api fetch request or live web socket stream
        :param timestamp: timestamp attached to incoming data
        :param metadata: contains the trading pair associated with the snapshot message
        :return: ZebpayOrderBookMessage
        """
        if metadata:
            msg.update(metadata)
        return ZebpayOrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        """
        *used for backtesting
        Convert a row of snapshot data into standard OrderBookMessage format
        :param record: a row of snapshot data from the database
        :param metadata: dictionary
        :return: ZebpayOrderBookMessage
        """
        return ZebpayOrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=record.json,
            timestamp=record.timestamp
        )

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None):
        """
        Convert JSON diff data into standard OrderBookMessage format
        :param msg: JSON diff data from live web socket stream
        :param timestamp: timestamp attached to incoming data
        :param metadata: trading pair associated with diff message
        :return: ZebpayOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        # TODO Brian: Require format for WS book messages to effectively parse incoming data.
        return ZebpayOrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        """
        *used for backtesting
        Convert a row of diff data into standard OrderBookMessage format
        :param record: a row of diff data from the database
        :return: ZebpayOrderBookMessage
        """
        return ZebpayOrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=record.json,
            timestamp=record.timestamp
        )

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, Any],
                                    timestamp: Optional[float] = None,
                                    metadata: Optional[Dict] = None):
        """
        Convert JSON trade data into standard OrderBookMessage format
        :param msg: JSON trade data from live web socket stream
        :param timestamp: timestamp attached to incoming data
        :param metadata: trading pair associated with trade message
        :return: ZebpayOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        # TODO Brian: Require format for WS trade messages to effectively parse incoming data.
        msg.update({
            "exchange_order_id": msg.get("d"),
            "trade_type": msg.get("s"),
            "price": msg.get("p"),
            "amount": msg.get("q"),
        })

        return ZebpayOrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def trade_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        """
        *used for backtesting
        Convert a row of trade data into standard OrderBookMessage format
        :param record: a row of trade data from the database
        :return: ZebpayOrderBookMessage
        """
        return ZebpayOrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=record.json,
            timestamp=record.timestamp
        )

    @classmethod
    def from_snapshot(cls, snapshot: OrderBookMessage):
        raise NotImplementedError("Zebpay orderbook needs to retain individual order data.")

    @classmethod
    def restore_from_snapshot_and_diffs(cls, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError("Zebpay orderbook needs to retain individual order data.")
