import asyncio

from decimal import Decimal
from typing import Any, Dict

from hummingbot.core.event.events import OrderType, TradeType
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


class ZebpayInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "open"):
        """
        :param client_order_id:
        :param exchange_order_id:
        :param trading_pair:
        :param order_type:
        :param trade_type:
        :param price:
        :param amount:
        :param initial_state:  open, partiallyFilled, filled, canceled, rejected
        """
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state,
        )
        self.fill_id_set = set()
        self.cancelled_event = asyncio.Event()

    @property
    def is_done(self) -> bool:
        # TODO: Status requires adjustment - no "rejected" status in Zebpay
        return self.last_state in {"filled", "canceled", "rejected"}

    @property
    def is_failure(self) -> bool:
        # TODO: Status requires adjustment - no "rejected" status in Zebpay
        return self.last_state in {"rejected", }

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"cancelled"}

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        """
        :param data: json data from API
        :return: formatted InFlightOrder
        """
        result = ZebpayInFlightOrder(
            data["client_order_id"],
            data["exchange_order_id"],
            data["trading_pair"],
            getattr(OrderType, data["order_type"]),
            getattr(TradeType, data["trade_type"]),
            Decimal(data["price"]),
            Decimal(data["amount"]),
            data["last_state"]
        )
        result.executed_amount_base = Decimal(data["executed_amount_base"])
        result.executed_amount_quote = Decimal(data["executed_amount_quote"])
        result.fee_asset = data["fee_asset"]
        result.fee_paid = Decimal(data["fee_paid"])  # TODO: Account for gas
        result.last_state = data["last_state"]
        return result

    def update_with_fill_update(self, fill_update: Dict[str, Any]) -> bool:
        # TODO: Update per Zebpay API: Zebpay does not provide partial fill information on API
        """
        Updates the in flight order with fill update (from private/get-order-detail end point)
        return: True if the order gets updated otherwise False
        """
        fill_id = fill_update["i"] if "i" in fill_update else fill_update.get("i")
        if fill_id in self.fill_id_set:
            # fill already recorded
            return False
        self.fill_id_set.add(fill_id)
        self.executed_amount_base += Decimal(str(fill_update["q"] if "q" in fill_update else
                                                 fill_update.get("quantity")))
        self.fee_paid += Decimal(str(fill_update["f"] if "f" in fill_update else fill_update.get("fee")))
        self.executed_amount_quote += (
            Decimal(str(fill_update["p"] if "p" in fill_update else fill_update.get("price"))) * Decimal(
                str(fill_update["q"] if "q" in fill_update else fill_update.get("quantity")))
        )
        if not self.fee_asset:
            self.fee_asset = fill_update["a"] if "a" in fill_update else fill_update.get("feeAsset")
        return True