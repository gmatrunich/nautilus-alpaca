"""Pure converters between Alpaca and NautilusTrader data objects.

These functions take Alpaca's pydantic models (``Quote``, ``Trade``, ``Bar``,
``TradeUpdate`` …) plus an already-resolved Nautilus ``Instrument`` and
return the corresponding Nautilus value. Keeping these as plain functions
makes them easy to unit-test and reuse from the live data client, the
historical loader, and the execution client.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from alpaca.data.models import Bar as AlpacaBar
from alpaca.data.models import Quote as AlpacaQuote
from alpaca.data.models import Trade as AlpacaTrade
from alpaca.data.timeframe import TimeFrame
from alpaca.data.timeframe import TimeFrameUnit

from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


_BAR_AGGREGATION_TO_TIMEFRAME_UNIT: dict[BarAggregation, TimeFrameUnit] = {
    BarAggregation.MINUTE: TimeFrameUnit.Minute,
    BarAggregation.HOUR: TimeFrameUnit.Hour,
    BarAggregation.DAY: TimeFrameUnit.Day,
    BarAggregation.WEEK: TimeFrameUnit.Week,
    BarAggregation.MONTH: TimeFrameUnit.Month,
}


# ─── Quotes ────────────────────────────────────────────────────────────────


def parse_quote(alpaca_quote: AlpacaQuote, instrument: Instrument) -> QuoteTick:
    ts = _ts_to_ns(alpaca_quote.timestamp)
    return QuoteTick(
        instrument_id=instrument.id,
        bid_price=_price(alpaca_quote.bid_price, instrument),
        ask_price=_price(alpaca_quote.ask_price, instrument),
        bid_size=_size(alpaca_quote.bid_size, instrument),
        ask_size=_size(alpaca_quote.ask_size, instrument),
        ts_event=ts,
        ts_init=ts,
    )


# ─── Trades ────────────────────────────────────────────────────────────────


def parse_trade(alpaca_trade: AlpacaTrade, instrument: Instrument) -> TradeTick:
    ts = _ts_to_ns(alpaca_trade.timestamp)
    trade_id = TradeId(str(alpaca_trade.id) if alpaca_trade.id is not None else _synthetic_trade_id(ts))
    return TradeTick(
        instrument_id=instrument.id,
        price=_price(alpaca_trade.price, instrument),
        size=_size(alpaca_trade.size, instrument),
        aggressor_side=AggressorSide.NO_AGGRESSOR,
        trade_id=trade_id,
        ts_event=ts,
        ts_init=ts,
    )


# ─── Bars ──────────────────────────────────────────────────────────────────


def parse_bar(
    alpaca_bar: AlpacaBar,
    bar_type: BarType,
    instrument: Instrument,
) -> Bar:
    ts = _ts_to_ns(alpaca_bar.timestamp)
    return Bar(
        bar_type=bar_type,
        open=_price(alpaca_bar.open, instrument),
        high=_price(alpaca_bar.high, instrument),
        low=_price(alpaca_bar.low, instrument),
        close=_price(alpaca_bar.close, instrument),
        volume=_size(alpaca_bar.volume, instrument),
        ts_event=ts,
        ts_init=ts,
    )


def bar_spec_to_timeframe(bar_type: BarType) -> TimeFrame:
    """Map a Nautilus ``BarType`` to an Alpaca ``TimeFrame``.

    Raises ``ValueError`` for unsupported aggregations.
    """
    spec = bar_type.spec
    unit = _BAR_AGGREGATION_TO_TIMEFRAME_UNIT.get(spec.aggregation)
    if unit is None:
        raise ValueError(
            f"Unsupported bar aggregation for Alpaca: {spec.aggregation!r} "
            "(supported: MINUTE, HOUR, DAY, WEEK, MONTH)",
        )
    if bar_type.aggregation_source != AggregationSource.EXTERNAL:
        raise ValueError(
            "Alpaca provides externally aggregated bars only; "
            "use INTERNAL aggregation against subscribed ticks for custom periods.",
        )
    return TimeFrame(amount=spec.step, unit=unit)


# ─── Helpers ───────────────────────────────────────────────────────────────


def _ts_to_ns(ts: datetime) -> int:
    return dt_to_unix_nanos(ts)


def _price(value: float | Decimal | None, instrument: Instrument) -> Price:
    if value is None:
        return Price(Decimal(0), instrument.price_precision)
    return Price(Decimal(str(value)), instrument.price_precision)


def _size(value: float | Decimal | None, instrument: Instrument) -> Quantity:
    precision = getattr(instrument, "size_precision", 0)
    if value is None:
        return Quantity(Decimal(0), precision)
    return Quantity(Decimal(str(value)), precision)


def _synthetic_trade_id(ts_ns: int) -> str:
    return f"alpaca-{ts_ns}"


__all__ = [
    "bar_spec_to_timeframe",
    "parse_bar",
    "parse_quote",
    "parse_trade",
]
