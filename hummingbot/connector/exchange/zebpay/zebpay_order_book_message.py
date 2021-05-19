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
        if self.type is OrderBookMessageType.SNAPSHOT:
            return int(self.timestamp)
        elif self.type is OrderBookMessageType.DIFF:
            return int(self.timestamp)
        else:
            return -1

    @property
    def trade_id(self) -> int:
        if self.type is OrderBookMessageType.TRADE:
            #  Transaction ID provided in History API responses as "trans_id".
            #  WS history response must provide the same key/value pair or below keys will require adjustment.
            '''
            Sample WS response:
            {
                    "trans_id": 13,
                    "fill_qty": 1000000,
                    "fill_price": 0.0005,
                    "fill_flags": 1,
                    "currencyPair": "BTC-AUD",
                    "lastModifiedDate": 1538576785865
                    
            }
            '''
            return int(self.content["trans_id"])
        return -1

    @property
    def trading_pair(self) -> str:
        # Assumes the same key/value pair between API and WS responses.
        '''
        {
            "asks": [],
            "bids": [
                {
                "price": "14962.44",
                "amount": 20044680
                }
            ],
            "pair": "btc-aud"
        }
        '''

        return self.content["pair"]

    @property
    def asks(self) -> List[OrderBookRow]:
        raise NotImplementedError("Zebpay orderbook messages have different semantics.")

    @property
    def bids(self) -> List[OrderBookRow]:
        raise NotImplementedError("Zebpay orderbook messages have different semantics.")