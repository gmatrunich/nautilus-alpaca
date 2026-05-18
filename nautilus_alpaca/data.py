"""Live market-data client for Alpaca.

Routes subscription commands to the correct WebSocket connection (stock,
crypto, or option) based on the instrument's symbol form, and forwards
parsed data into NautilusTrader's data bus via ``self._handle_data``.

REST request handlers (historical bars/quotes/trades) are dispatched to
the corresponding ``AlpacaHttpClient`` method and the result list is
returned through ``self._handle_*`` callbacks.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

from alpaca.data.enums import CryptoFeed
from alpaca.data.enums import DataFeed
from alpaca.data.enums import OptionsFeed
from alpaca.data.models import Bar as AlpacaBar
from alpaca.data.models import Quote as AlpacaQuote
from alpaca.data.models import Trade as AlpacaTrade

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.data.messages import RequestBars
from nautilus_trader.data.messages import RequestInstrument
from nautilus_trader.data.messages import RequestInstruments
from nautilus_trader.data.messages import RequestQuoteTicks
from nautilus_trader.data.messages import RequestTradeTicks
from nautilus_trader.data.messages import SubscribeBars
from nautilus_trader.data.messages import SubscribeInstrument
from nautilus_trader.data.messages import SubscribeInstruments
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import SubscribeTradeTicks
from nautilus_trader.data.messages import UnsubscribeBars
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.identifiers import InstrumentId

from nautilus_alpaca.common.constants import ALPACA_CLIENT_ID
from nautilus_alpaca.common.constants import ALPACA_VENUE
from nautilus_alpaca.common.symbols import is_crypto_symbol
from nautilus_alpaca.common.symbols import is_option_symbol
from nautilus_alpaca.config import AlpacaDataClientConfig
from nautilus_alpaca.http.client import AlpacaHttpClient
from nautilus_alpaca.parsers import bar_spec_to_timeframe
from nautilus_alpaca.parsers import parse_bar
from nautilus_alpaca.parsers import parse_quote
from nautilus_alpaca.parsers import parse_trade
from nautilus_alpaca.providers import AlpacaInstrumentProvider
from nautilus_alpaca.websocket.data import AlpacaDataWebSocket
from nautilus_alpaca.websocket.data import StreamKind


class AlpacaDataClient(LiveMarketDataClient):
    """Live Nautilus data client for Alpaca markets."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        http_client: AlpacaHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: AlpacaInstrumentProvider,
        config: AlpacaDataClientConfig,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ALPACA_CLIENT_ID,
            venue=ALPACA_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        self._http = http_client
        self._config = config

        self._stock_ws: AlpacaDataWebSocket | None = None
        self._crypto_ws: AlpacaDataWebSocket | None = None
        self._option_ws: AlpacaDataWebSocket | None = None

        if config.use_stock_stream:
            self._stock_ws = AlpacaDataWebSocket(
                kind=StreamKind.STOCK,
                api_key=http_client.api_key,
                api_secret=http_client.api_secret,
                stock_feed=DataFeed(config.stock_feed),
                url_override=config.stock_ws_url_override,
            )
        if config.use_crypto_stream:
            self._crypto_ws = AlpacaDataWebSocket(
                kind=StreamKind.CRYPTO,
                api_key=http_client.api_key,
                api_secret=http_client.api_secret,
                crypto_feed=CryptoFeed(config.crypto_feed),
                url_override=config.crypto_ws_url_override,
            )
        if config.use_option_stream:
            self._option_ws = AlpacaDataWebSocket(
                kind=StreamKind.OPTION,
                api_key=http_client.api_key,
                api_secret=http_client.api_secret,
                option_feed=OptionsFeed(config.option_feed),
                url_override=config.option_ws_url_override,
            )

        # Track active subscriptions per stream so we can resubscribe after reconnect
        self._active_quote_subs: dict[StreamKind, set[str]] = defaultdict(set)
        self._active_trade_subs: dict[StreamKind, set[str]] = defaultdict(set)
        self._active_bar_subs: dict[StreamKind, set[str]] = defaultdict(set)

    # ─── Lifecycle ─────────────────────────────────────────────────────────

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()
        for instrument in self._instrument_provider.list_all():
            self._handle_data(instrument)

        coros = []
        if self._stock_ws is not None:
            coros.append(self._stock_ws.start())
        if self._crypto_ws is not None:
            coros.append(self._crypto_ws.start())
        if self._option_ws is not None:
            coros.append(self._option_ws.start())
        if coros:
            await asyncio.gather(*coros)
        self._log.info("AlpacaDataClient connected")

    async def _disconnect(self) -> None:
        coros = []
        for ws in (self._stock_ws, self._crypto_ws, self._option_ws):
            if ws is not None:
                coros.append(ws.stop())
        if coros:
            await asyncio.gather(*coros, return_exceptions=True)
        self._log.info("AlpacaDataClient disconnected")

    # ─── Subscriptions: instruments ────────────────────────────────────────

    async def _subscribe_instruments(self, command: SubscribeInstruments) -> None:
        # Provider is already initialized on connect; nothing to push live
        # (Alpaca doesn't stream instrument-master updates over WS).
        pass

    async def _subscribe_instrument(self, command: SubscribeInstrument) -> None:
        await self._instrument_provider.load_async(command.instrument_id)
        instrument = self._instrument_provider.find(command.instrument_id)
        if instrument is not None:
            self._handle_data(instrument)

    # ─── Subscriptions: quotes / trades / bars ────────────────────────────

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        symbol, kind, ws = self._resolve_stream(command.instrument_id)
        if ws is None:
            return
        ws.subscribe_quotes(self._on_alpaca_quote, symbol)
        self._active_quote_subs[kind].add(symbol)

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        symbol, kind, ws = self._resolve_stream(command.instrument_id)
        if ws is None:
            return
        ws.subscribe_trades(self._on_alpaca_trade, symbol)
        self._active_trade_subs[kind].add(symbol)

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        bar_type = command.bar_type
        symbol, kind, ws = self._resolve_stream(bar_type.instrument_id)
        if ws is None:
            return
        # Alpaca only streams 1-minute aggregates over WS — validate eagerly.
        from nautilus_trader.model.enums import BarAggregation
        spec = bar_type.spec
        if not (spec.aggregation == BarAggregation.MINUTE and spec.step == 1):
            self._log.warning(
                f"Alpaca WS only streams 1-minute bars; got {bar_type}. "
                "Subscribe to trades and aggregate internally instead.",
            )
            return
        ws.subscribe_bars(self._make_bar_handler(bar_type), symbol)
        self._active_bar_subs[kind].add(symbol)

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        symbol, kind, ws = self._resolve_stream(command.instrument_id)
        if ws is None:
            return
        ws.unsubscribe_quotes(symbol)
        self._active_quote_subs[kind].discard(symbol)

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        symbol, kind, ws = self._resolve_stream(command.instrument_id)
        if ws is None:
            return
        ws.unsubscribe_trades(symbol)
        self._active_trade_subs[kind].discard(symbol)

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        symbol, kind, ws = self._resolve_stream(command.bar_type.instrument_id)
        if ws is None:
            return
        ws.unsubscribe_bars(symbol)
        self._active_bar_subs[kind].discard(symbol)

    # ─── Live message handlers ────────────────────────────────────────────

    async def _on_alpaca_quote(self, quote: AlpacaQuote) -> None:
        instrument_id = self._instrument_id_for(quote.symbol)
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.warning(f"Received quote for unknown instrument: {quote.symbol}")
            return
        self._handle_data(parse_quote(quote, instrument))

    async def _on_alpaca_trade(self, trade: AlpacaTrade) -> None:
        instrument_id = self._instrument_id_for(trade.symbol)
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.warning(f"Received trade for unknown instrument: {trade.symbol}")
            return
        self._handle_data(parse_trade(trade, instrument))

    def _make_bar_handler(self, bar_type) -> Any:
        async def _handler(bar: AlpacaBar) -> None:
            instrument = self._instrument_provider.find(bar_type.instrument_id)
            if instrument is None:
                self._log.warning(f"Received bar for unknown instrument: {bar.symbol}")
                return
            self._handle_data(parse_bar(bar, bar_type, instrument))
        return _handler

    # ─── Historical requests ──────────────────────────────────────────────

    async def _request_instrument(self, request: RequestInstrument) -> None:
        await self._instrument_provider.load_async(request.instrument_id)
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            self._log.error(f"Could not resolve instrument {request.instrument_id}")
            return
        self._handle_instrument(
            instrument,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_instruments(self, request: RequestInstruments) -> None:
        await self._instrument_provider.initialize()
        instruments = self._instrument_provider.list_all()
        self._handle_instruments(
            request.venue,
            instruments,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            await self._instrument_provider.load_async(request.instrument_id)
            instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            self._log.error(f"Unknown instrument: {request.instrument_id}")
            return
        symbol = request.instrument_id.symbol.value
        if is_option_symbol(symbol):
            self._log.warning("Alpaca does not expose historical option quotes")
            return
        if is_crypto_symbol(symbol):
            response = await self._http.get_crypto_quotes(
                symbol, request.start, request.end, request.limit,
            )
        else:
            response = await self._http.get_stock_quotes(
                symbol, request.start, request.end, request.limit,
            )
        raw_quotes = self._extract_symbol_series(response, symbol)
        ticks = [parse_quote(q, instrument) for q in raw_quotes]
        self._handle_quote_ticks(
            request.instrument_id,
            ticks,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_trade_ticks(self, request: RequestTradeTicks) -> None:
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            await self._instrument_provider.load_async(request.instrument_id)
            instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            self._log.error(f"Unknown instrument: {request.instrument_id}")
            return
        symbol = request.instrument_id.symbol.value
        if is_option_symbol(symbol):
            response = await self._http.get_option_trades(
                symbol, request.start, request.end, request.limit,
            )
        elif is_crypto_symbol(symbol):
            response = await self._http.get_crypto_trades(
                symbol, request.start, request.end, request.limit,
            )
        else:
            response = await self._http.get_stock_trades(
                symbol, request.start, request.end, request.limit,
            )
        raw_trades = self._extract_symbol_series(response, symbol)
        ticks = [parse_trade(t, instrument) for t in raw_trades]
        self._handle_trade_ticks(
            request.instrument_id,
            ticks,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_bars(self, request: RequestBars) -> None:
        bar_type = request.bar_type
        instrument_id = bar_type.instrument_id
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            await self._instrument_provider.load_async(instrument_id)
            instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(f"Unknown instrument: {instrument_id}")
            return
        symbol = instrument_id.symbol.value
        timeframe = bar_spec_to_timeframe(bar_type)
        if is_option_symbol(symbol):
            response = await self._http.get_option_bars(
                symbol, timeframe, request.start, request.end, request.limit,
            )
        elif is_crypto_symbol(symbol):
            response = await self._http.get_crypto_bars(
                symbol, timeframe, request.start, request.end, request.limit,
            )
        else:
            response = await self._http.get_stock_bars(
                symbol, timeframe, request.start, request.end, request.limit,
            )
        raw_bars = self._extract_symbol_series(response, symbol)
        bars = [parse_bar(b, bar_type, instrument) for b in raw_bars]
        self._handle_bars(
            bar_type,
            bars,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    # ─── Routing helpers ──────────────────────────────────────────────────

    def _resolve_stream(
        self,
        instrument_id: InstrumentId,
    ) -> tuple[str, StreamKind, AlpacaDataWebSocket | None]:
        symbol = instrument_id.symbol.value
        if is_option_symbol(symbol):
            ws = self._option_ws
            kind = StreamKind.OPTION
        elif is_crypto_symbol(symbol):
            ws = self._crypto_ws
            kind = StreamKind.CRYPTO
        else:
            ws = self._stock_ws
            kind = StreamKind.STOCK
        if ws is None:
            self._log.warning(
                f"No live stream configured for {kind.value} (instrument {instrument_id}); "
                f"set use_{kind.value}_stream=True on AlpacaDataClientConfig.",
            )
        return symbol, kind, ws

    def _instrument_id_for(self, symbol: str) -> InstrumentId:
        from nautilus_alpaca.common.symbols import instrument_id_from_alpaca_symbol
        return instrument_id_from_alpaca_symbol(symbol)

    @staticmethod
    def _extract_symbol_series(response: Any, symbol: str) -> list[Any]:
        """alpaca-py returns a ``BarSet`` / ``QuoteSet`` / ``TradeSet`` whose
        ``.data`` is ``dict[symbol → list]`` for batched requests. For a
        single symbol the same structure is used.
        """
        data = getattr(response, "data", response)
        if isinstance(data, dict):
            return list(data.get(symbol, []))
        return list(data)
