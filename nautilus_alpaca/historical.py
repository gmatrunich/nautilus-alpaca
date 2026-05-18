"""Standalone helper for downloading historical Alpaca data for backtests.

Returns plain Python lists of NautilusTrader ``Bar`` / ``QuoteTick`` /
``TradeTick`` objects (no event loop required from the caller — the helper
internally uses ``asyncio.run`` if it isn't already on a loop).

Typical usage::

    from datetime import datetime, timezone
    from nautilus_alpaca import AlpacaHistoricalDataLoader

    loader = AlpacaHistoricalDataLoader.from_env(paper=True)
    bars = loader.bars_sync(
        bar_type="AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL",
        start=datetime(2024, 1, 2, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, tzinfo=timezone.utc),
    )
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument

from nautilus_alpaca.common.credentials import load_credentials
from nautilus_alpaca.common.symbols import is_crypto_symbol
from alpaca.data.enums import CryptoFeed
from alpaca.data.enums import DataFeed

from nautilus_alpaca.common.symbols import is_option_symbol
from nautilus_alpaca.http.client import AlpacaHttpClient
from nautilus_alpaca.parsers import bar_spec_to_timeframe
from nautilus_alpaca.parsers import parse_bar
from nautilus_alpaca.parsers import parse_quote
from nautilus_alpaca.parsers import parse_trade
from nautilus_alpaca.providers import AlpacaInstrumentProvider


class AlpacaHistoricalDataLoader:
    """Async helper for pulling historical bars/quotes/trades from Alpaca.

    The loader owns its own ``AlpacaHttpClient`` and ``AlpacaInstrumentProvider``
    so it can be used outside of a ``TradingNode``.
    """

    def __init__(
        self,
        http_client: AlpacaHttpClient,
        instrument_provider: AlpacaInstrumentProvider | None = None,
        stock_feed: DataFeed = DataFeed.IEX,
        crypto_feed: CryptoFeed = CryptoFeed.US,
    ) -> None:
        self._http = http_client
        self._provider = instrument_provider or AlpacaInstrumentProvider(
            client=http_client,
            config=InstrumentProviderConfig(load_all=False),
        )
        self._stock_feed = stock_feed
        self._crypto_feed = crypto_feed

    # ─── Constructors ──────────────────────────────────────────────────────

    @classmethod
    def from_env(
        cls,
        paper: bool = True,
        api_key: str | None = None,
        api_secret: str | None = None,
    ) -> AlpacaHistoricalDataLoader:
        credentials = load_credentials(api_key=api_key, api_secret=api_secret, paper=paper)
        http = AlpacaHttpClient(
            api_key=credentials.api_key,
            api_secret=credentials.api_secret,
            paper=credentials.paper,
        )
        return cls(http_client=http)

    # ─── Async API ─────────────────────────────────────────────────────────

    async def instrument(self, instrument_id: InstrumentId | str) -> Instrument:
        instrument_id = _ensure_id(instrument_id)
        instrument = self._provider.find(instrument_id)
        if instrument is None:
            await self._provider.load_async(instrument_id)
            instrument = self._provider.find(instrument_id)
        if instrument is None:
            raise LookupError(f"Could not resolve instrument {instrument_id}")
        return instrument

    async def bars(
        self,
        bar_type: BarType | str,
        start: datetime,
        end: datetime,
        limit: int | None = None,
    ) -> list[Bar]:
        bar_type = _ensure_bar_type(bar_type)
        instrument = await self.instrument(bar_type.instrument_id)
        symbol = bar_type.instrument_id.symbol.value
        timeframe = bar_spec_to_timeframe(bar_type)
        if is_option_symbol(symbol):
            resp = await self._http.get_option_bars(symbol, timeframe, start, end, limit)
        elif is_crypto_symbol(symbol):
            resp = await self._http.get_crypto_bars(symbol, timeframe, start, end, limit)
        else:
            resp = await self._http.get_stock_bars(symbol, timeframe, start, end, limit, feed=self._stock_feed)
        raw = _extract_symbol_series(resp, symbol)
        return [parse_bar(b, bar_type, instrument) for b in raw]

    async def quote_ticks(
        self,
        instrument_id: InstrumentId | str,
        start: datetime,
        end: datetime,
        limit: int | None = None,
    ) -> list[QuoteTick]:
        instrument_id = _ensure_id(instrument_id)
        instrument = await self.instrument(instrument_id)
        symbol = instrument_id.symbol.value
        if is_option_symbol(symbol):
            raise NotImplementedError(
                "Alpaca does not expose historical option quotes via this API",
            )
        if is_crypto_symbol(symbol):
            resp = await self._http.get_crypto_quotes(symbol, start, end, limit)
        else:
            resp = await self._http.get_stock_quotes(symbol, start, end, limit, feed=self._stock_feed)
        raw = _extract_symbol_series(resp, symbol)
        return [parse_quote(q, instrument) for q in raw]

    async def trade_ticks(
        self,
        instrument_id: InstrumentId | str,
        start: datetime,
        end: datetime,
        limit: int | None = None,
    ) -> list[TradeTick]:
        instrument_id = _ensure_id(instrument_id)
        instrument = await self.instrument(instrument_id)
        symbol = instrument_id.symbol.value
        if is_option_symbol(symbol):
            resp = await self._http.get_option_trades(symbol, start, end, limit)
        elif is_crypto_symbol(symbol):
            resp = await self._http.get_crypto_trades(symbol, start, end, limit)
        else:
            resp = await self._http.get_stock_trades(symbol, start, end, limit, feed=self._stock_feed)
        raw = _extract_symbol_series(resp, symbol)
        return [parse_trade(t, instrument) for t in raw]

    # ─── Sync wrappers ─────────────────────────────────────────────────────

    def bars_sync(self, *args: Any, **kwargs: Any) -> list[Bar]:
        return _run(self.bars(*args, **kwargs))

    def quote_ticks_sync(self, *args: Any, **kwargs: Any) -> list[QuoteTick]:
        return _run(self.quote_ticks(*args, **kwargs))

    def trade_ticks_sync(self, *args: Any, **kwargs: Any) -> list[TradeTick]:
        return _run(self.trade_ticks(*args, **kwargs))

    def instrument_sync(self, instrument_id: InstrumentId | str) -> Instrument:
        return _run(self.instrument(instrument_id))


# ─── Helpers ───────────────────────────────────────────────────────────────


def _ensure_id(value: InstrumentId | str) -> InstrumentId:
    return value if isinstance(value, InstrumentId) else InstrumentId.from_str(value)


def _ensure_bar_type(value: BarType | str) -> BarType:
    return value if isinstance(value, BarType) else BarType.from_str(value)


def _extract_symbol_series(response: Any, symbol: str) -> list[Any]:
    data = getattr(response, "data", response)
    if isinstance(data, dict):
        return list(data.get(symbol, []))
    return list(data)


def _run(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError(
        "Sync wrapper called from within a running event loop; await the async API instead.",
    )


__all__ = ["AlpacaHistoricalDataLoader"]
