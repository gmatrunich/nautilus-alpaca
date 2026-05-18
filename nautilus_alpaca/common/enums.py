"""Mapping between Alpaca and NautilusTrader enums."""
from __future__ import annotations

from alpaca.trading.enums import OrderClass as AlpacaOrderClass
from alpaca.trading.enums import OrderSide as AlpacaOrderSide
from alpaca.trading.enums import OrderStatus as AlpacaOrderStatus
from alpaca.trading.enums import OrderType as AlpacaOrderType
from alpaca.trading.enums import PositionSide as AlpacaPositionSide
from alpaca.trading.enums import TimeInForce as AlpacaTimeInForce

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce


# ─── Alpaca → Nautilus ──────────────────────────────────────────────────────

ALPACA_TO_NAUTILUS_ORDER_SIDE: dict[AlpacaOrderSide, OrderSide] = {
    AlpacaOrderSide.BUY: OrderSide.BUY,
    AlpacaOrderSide.SELL: OrderSide.SELL,
}

ALPACA_TO_NAUTILUS_ORDER_TYPE: dict[AlpacaOrderType, OrderType] = {
    AlpacaOrderType.MARKET: OrderType.MARKET,
    AlpacaOrderType.LIMIT: OrderType.LIMIT,
    AlpacaOrderType.STOP: OrderType.STOP_MARKET,
    AlpacaOrderType.STOP_LIMIT: OrderType.STOP_LIMIT,
    AlpacaOrderType.TRAILING_STOP: OrderType.TRAILING_STOP_MARKET,
}

ALPACA_TO_NAUTILUS_TIF: dict[AlpacaTimeInForce, TimeInForce] = {
    AlpacaTimeInForce.DAY: TimeInForce.DAY,
    AlpacaTimeInForce.GTC: TimeInForce.GTC,
    AlpacaTimeInForce.OPG: TimeInForce.AT_THE_OPEN,
    AlpacaTimeInForce.CLS: TimeInForce.AT_THE_CLOSE,
    AlpacaTimeInForce.IOC: TimeInForce.IOC,
    AlpacaTimeInForce.FOK: TimeInForce.FOK,
}

# Alpaca's OrderStatus has many states; collapse to Nautilus's narrower set.
ALPACA_TO_NAUTILUS_ORDER_STATUS: dict[AlpacaOrderStatus, OrderStatus] = {
    AlpacaOrderStatus.NEW: OrderStatus.ACCEPTED,
    AlpacaOrderStatus.PENDING_NEW: OrderStatus.SUBMITTED,
    AlpacaOrderStatus.ACCEPTED: OrderStatus.ACCEPTED,
    AlpacaOrderStatus.ACCEPTED_FOR_BIDDING: OrderStatus.ACCEPTED,
    AlpacaOrderStatus.PENDING_REVIEW: OrderStatus.SUBMITTED,
    AlpacaOrderStatus.HELD: OrderStatus.ACCEPTED,
    AlpacaOrderStatus.PARTIALLY_FILLED: OrderStatus.PARTIALLY_FILLED,
    AlpacaOrderStatus.FILLED: OrderStatus.FILLED,
    AlpacaOrderStatus.DONE_FOR_DAY: OrderStatus.CANCELED,
    AlpacaOrderStatus.CANCELED: OrderStatus.CANCELED,
    AlpacaOrderStatus.PENDING_CANCEL: OrderStatus.PENDING_CANCEL,
    AlpacaOrderStatus.EXPIRED: OrderStatus.EXPIRED,
    AlpacaOrderStatus.REPLACED: OrderStatus.ACCEPTED,
    AlpacaOrderStatus.PENDING_REPLACE: OrderStatus.PENDING_UPDATE,
    AlpacaOrderStatus.REJECTED: OrderStatus.REJECTED,
    AlpacaOrderStatus.SUSPENDED: OrderStatus.ACCEPTED,
    AlpacaOrderStatus.STOPPED: OrderStatus.ACCEPTED,
    AlpacaOrderStatus.CALCULATED: OrderStatus.FILLED,
}

ALPACA_TO_NAUTILUS_POSITION_SIDE: dict[AlpacaPositionSide, PositionSide] = {
    AlpacaPositionSide.LONG: PositionSide.LONG,
    AlpacaPositionSide.SHORT: PositionSide.SHORT,
}


# ─── Nautilus → Alpaca ──────────────────────────────────────────────────────

NAUTILUS_TO_ALPACA_ORDER_SIDE: dict[OrderSide, AlpacaOrderSide] = {
    OrderSide.BUY: AlpacaOrderSide.BUY,
    OrderSide.SELL: AlpacaOrderSide.SELL,
}

NAUTILUS_TO_ALPACA_TIF: dict[TimeInForce, AlpacaTimeInForce] = {
    TimeInForce.DAY: AlpacaTimeInForce.DAY,
    TimeInForce.GTC: AlpacaTimeInForce.GTC,
    TimeInForce.IOC: AlpacaTimeInForce.IOC,
    TimeInForce.FOK: AlpacaTimeInForce.FOK,
    TimeInForce.AT_THE_OPEN: AlpacaTimeInForce.OPG,
    TimeInForce.AT_THE_CLOSE: AlpacaTimeInForce.CLS,
}


def parse_alpaca_order_class(value: str | None) -> AlpacaOrderClass:
    if value is None:
        return AlpacaOrderClass.SIMPLE
    return AlpacaOrderClass(value)
