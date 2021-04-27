import asyncio
import logging
import socketio
import time
from typing import (
    Any,
    Dict,
    Optional,
)

from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.zebpay.zebpay_resolve import (
    BUY_MSG,
    BUY_ACTION_TYPE,
    SELL_MSG,
    SELL_ACTION_TYPE,
    TRADE_MSG,
    DIFF_MSG,
    ADD_MSG,
    DELETE_MSG,

    ORDER_FAILED,
    ORDER_PENDING,
    ORDER_FINISHED,
    ORDER_CANCELLED,
)
from hummingbot.connector.exchange.zebpay.zebpay_order_book import ZebpayOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage


class ZebpayCustomNamespace(socketio.AsyncClientNamespace):

    _zcns_logger: Optional[HummingbotLogger] = None
    _trade_queue: asyncio.Queue = None
    _diff_queue: asyncio.Queue = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._zcns_logger is None:
            cls._zcns_logger = logging.getLogger(__name__)
        return cls._zcns_logger

    def __init__(self):
        super().__init__()

    def on_connect(self):
        self.logger().info("Websocket connected.")

    def on_disconnect(self):
        self.logger().info("Websocket disconnected.")

    def on_connect_error(self):
        self.logger().info("Websocket connection failed.")

    def set_diff_queue(self, output):
        if self._diff_queue is None:
            self._diff_queue = output

    def diff_handler(self, pair, action, side, args):
        # ignore message if queue isn't ready
        if self._diff_queue is None:
            return

        # ignore ADD messages, as Zebpay only sends the individual order data, not a full diff
        if action == ADD_MSG:
            return

        msg: Dict[str, Any] = {}
        msg["pair"] = pair
        if action == DELETE_MSG:
            msg["updatedAt"] = time.time()
        else:
            msg["updatedAt"] = args[0][0]["updatedAt"]

        # zebpay only sends bid data with buy messages
        if side == BUY_MSG:
            msg["bids"] = [list(item.values()) for item in args[0]]
            msg["asks"] = []
        # zebpay only sends ask data with sell messages
        if side == SELL_MSG:
            msg["asks"] = [list(item.values()) for item in args[0]]
            msg["bids"] = []
        diff_msg: OrderBookMessage = ZebpayOrderBook.diff_message_from_exchange(msg)
        return self._diff_queue.put_nowait(diff_msg)

    def set_trade_queue(self, output):
        if self._trade_queue is None:
            self._trade_queue = output

    def trade_handler(self, pair, args):
        # ignore message if queue isn't ready
        if self._trade_queue is None:
            return

        msg: Dict[str, Any] = args[0][0]
        msg["pair"] = pair
        trade_msg: OrderBookMessage = ZebpayOrderBook.trade_message_from_exchange(msg)
        return self._trade_queue.put_nowait(trade_msg)

    async def trigger_event(self, event, *args):
        """Dispatch an event to the proper handler method.
        Note: this method is a coroutine.
        """
        handler_name = 'on_' + event
        if hasattr(self, handler_name):  # if it's a built-in event like connect or disconnect
            return getattr(self, handler_name)(*args)
        else:
            # Format is:
            # EVENT_PAIR_INFO_BY_SYMBOL_<QUOTE>_<BASE>      or
            # <QUOTE>_<BASE>EVENT_MATCH      or
            # <QUOTE>_<BASE>EVENT_<ADD|DELETE>_ORDER_<BUY|SELL>
            try:
                pair, event_type = event.split("EVENT_")
            except Exception:
                self.logger().debug(f"Unable to split websocket message: {event}. Discarding.")
                return
            if pair == "":
                # do nothing with INFO messages
                return
            else:
                if event_type.endswith("MATCH"):
                    return self.trade_handler(pair, args)
                else:
                    try:
                        action, side = event_type.split("_ORDER_")
                    except Exception:
                        self.logger().debug(f"Unable to split websocket message: {event_type}. Discarding.")
                        return
                    # must be a diff
                    return self.diff_handler(pair, action, side, args)
