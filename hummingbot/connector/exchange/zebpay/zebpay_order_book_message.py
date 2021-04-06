#!/usr/bin/env python

import pandas as pd
from typing import (
    Dict,
    List,
    Optional,
)

from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType,
)


class ZebpayOrderBookMessage(OrderBookMessage):
    def __new__(
        cls,
        message_type: OrderBookMessageType,
        content: Dict[str, any],
        timestamp: Optional[float] = None,
        *args,
        **kwargs,
    ):
        if timestamp is None:
            if message_type is OrderBookMessageType.SNAPSHOT:
                raise ValueError("timestamp must not be None when initializing snapshot messages.")
            timestamp = pd.Timestamp(content["data"]["t"], unit="ms").timestamp()
        return super(ZebpayOrderBookMessage, cls).__new__(
            cls, message_type, content, timestamp=timestamp, *args, **kwargs
        )

    @property
    def update_id(self) -> int:
        # TODO: Must rely on timestamps for comparison between snapshot and diff recency. These may be merged into one
        #  statement in the future.
        if self.type is OrderBookMessageType.SNAPSHOT:
            return int(self.timestamp)
        elif self.type is OrderBookMessageType.DIFF:
            return int(self.timestamp)
        else:
            return -1

    @property
    def trade_id(self) -> int:
        if self.type is OrderBookMessageType.TRADE:
            # TODO: Transaction ID provided in History API responses as "trans_id".
            #  Confirm the same is provided in WS history responses.
            return int(self.content["data"]["u"])
        return -1

    @property
    def trading_pair(self) -> str:
        # TODO Brian: Confirm key/value pair for trading pairs in DIFF/TRADE/SNAPSHOT messages
        # Trading pairs in DIFF/TRADE orderbook messages found in self.content[TBD].
        # Trading pairs in SNAPSHOT orderbook messages found in self.content[TBD].
        if self.content.get("data"):
            return self.content["data"]["m"]
        else:
            return self.content["trading_pair"]

    @property
    def asks(self) -> List[OrderBookRow]:
        raise NotImplementedError("Zebpay orderbook messages have different semantics.")

    @property
    def bids(self) -> List[OrderBookRow]:
        raise NotImplementedError("Zebpay orderbook messages have different semantics.")