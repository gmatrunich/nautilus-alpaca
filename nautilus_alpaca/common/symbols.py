"""Symbol parsing/normalization between Alpaca and NautilusTrader.

Alpaca symbol formats:
- Equities: plain ticker (e.g. ``AAPL``, ``MSFT``)
- Crypto:   ``BASE/QUOTE`` (e.g. ``BTC/USD``, ``ETH/USDT``)
- Options:  OCC 21-char (e.g. ``AAPL250117C00150000``)

NautilusTrader instrument IDs are ``"<raw>.ALPACA"`` where ``<raw>`` is the
exact Alpaca symbol. The ``/`` in crypto symbols is preserved in the raw
symbol component (NautilusTrader ``Symbol`` accepts it).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from decimal import Decimal

from nautilus_trader.model.enums import OptionKind
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol

from nautilus_alpaca.common.constants import ALPACA_VENUE


_OCC_PATTERN = re.compile(r"^(?P<root>[A-Z\.]{1,6})(?P<expiry>\d{6})(?P<kind>[CP])(?P<strike>\d{8})$")


@dataclass(frozen=True)
class OccComponents:
    """Decomposed OCC 21-character option symbol."""

    root: str
    expiration: datetime
    kind: OptionKind
    strike: Decimal


def parse_occ_symbol(symbol: str) -> OccComponents:
    """Decompose an OCC 21-character option symbol into its components.

    Parameters
    ----------
    symbol : str
        Symbol such as ``"AAPL250117C00150000"``.

    Raises
    ------
    ValueError
        If ``symbol`` does not match the OCC pattern.
    """
    match = _OCC_PATTERN.match(symbol)
    if match is None:
        raise ValueError(f"Not a valid OCC option symbol: {symbol!r}")
    yy = int(match["expiry"][0:2])
    mm = int(match["expiry"][2:4])
    dd = int(match["expiry"][4:6])
    # OCC uses 2-digit years; treat 00-49 as 2000-2049, 50-99 as 1950-1999.
    year = 2000 + yy if yy < 50 else 1900 + yy
    expiration = datetime(year, mm, dd, tzinfo=timezone.utc)
    kind = OptionKind.CALL if match["kind"] == "C" else OptionKind.PUT
    # Strike is encoded as price * 1000 in an 8-digit zero-padded field.
    strike = Decimal(match["strike"]) / Decimal(1000)
    return OccComponents(
        root=match["root"],
        expiration=expiration,
        kind=kind,
        strike=strike,
    )


def is_crypto_symbol(symbol: str) -> bool:
    """Crypto symbols use ``BASE/QUOTE`` notation."""
    return "/" in symbol


def is_option_symbol(symbol: str) -> bool:
    """OCC symbols are 15-21 chars and end with strike digits."""
    return _OCC_PATTERN.match(symbol) is not None


def instrument_id_from_alpaca_symbol(symbol: str) -> InstrumentId:
    """Build a ``InstrumentId`` for an Alpaca symbol on the ALPACA venue."""
    return InstrumentId(symbol=Symbol(symbol), venue=ALPACA_VENUE)


def alpaca_symbol_from_instrument_id(instrument_id: InstrumentId) -> str:
    """Extract the raw Alpaca symbol from a NautilusTrader ``InstrumentId``."""
    return instrument_id.symbol.value
