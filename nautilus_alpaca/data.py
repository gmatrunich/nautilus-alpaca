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

from alpaca.data.enums import Adjustment
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
from nautilus_trader.model.data import BarType
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

        # Daily-bar polling (Alpaca WS doesn't push daily bars). One task,
        # batched multi-symbol REST per asset class.
        self._polled_bar_subs: dict[BarType, "Any"] = {}  # bar_type → instrument
        self._polled_bar_last_ts: dict[BarType, int] = {}
        self._polled_bar_sub_ns: dict[BarType, int] = {}  # subscribe-time anchor
        # On the first poll for a freshly-registered bar_type we seed
        # ``_polled_bar_last_ts`` to the latest bar timestamp in the
        # response and *don't* dispatch — historical warmup is the
        # strategy's job via ``request_bars``. Subsequent polls
        # dispatch only strictly-newer bars.
        self._polled_bar_seeded: set[BarType] = set()
        self._poll_task: asyncio.Task | None = None

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
        # Daily-bar poller idles until something subscribes.
        self._poll_task = asyncio.create_task(
            self._daily_bar_poll_loop(),
            name="alpaca-daily-bar-poll",
        )
        self._log.info("AlpacaDataClient connected")

    async def _disconnect(self) -> None:
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._poll_task = None
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
        from nautilus_trader.model.enums import BarAggregation
        bar_type = command.bar_type
        spec = bar_type.spec
        instrument_id = bar_type.instrument_id

        # Daily (and larger) bars: Alpaca WS doesn't push these, so poll REST.
        if spec.aggregation in (
            BarAggregation.DAY,
            BarAggregation.WEEK,
            BarAggregation.MONTH,
        ):
            instrument = self._instrument_provider.find(instrument_id)
            if instrument is None:
                await self._instrument_provider.load_async(instrument_id)
                instrument = self._instrument_provider.find(instrument_id)
            if instrument is None:
                self._log.warning(
                    f"Cannot poll daily bars for {instrument_id}: instrument unknown",
                )
                return
            self._polled_bar_subs[bar_type] = instrument
            self._polled_bar_sub_ns[bar_type] = self._clock.timestamp_ns()
            self._log.info(
                f"Registered {bar_type} for REST polling "
                f"(interval {self._config.daily_poll_interval_secs:.0f}s)",
            )
            return

        # 1-minute bars: use the WebSocket stream.
        symbol, kind, ws = self._resolve_stream(instrument_id)
        if ws is None:
            return
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
        bar_type = command.bar_type
        # Polled daily/weekly/monthly bar subscription?
        if bar_type in self._polled_bar_subs:
            self._polled_bar_subs.pop(bar_type, None)
            self._polled_bar_last_ts.pop(bar_type, None)
            self._polled_bar_sub_ns.pop(bar_type, None)
            self._polled_bar_seeded.discard(bar_type)
            return
        symbol, kind, ws = self._resolve_stream(bar_type.instrument_id)
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
                feed=CryptoFeed(self._config.crypto_feed),
            )
        else:
            response = await self._http.get_stock_quotes(
                symbol, request.start, request.end, request.limit,
                feed=DataFeed(self._config.stock_feed),
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
                feed=CryptoFeed(self._config.crypto_feed),
            )
        else:
            response = await self._http.get_stock_trades(
                symbol, request.start, request.end, request.limit,
                feed=DataFeed(self._config.stock_feed),
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
                feed=CryptoFeed(self._config.crypto_feed),
            )
        else:
            response = await self._http.get_stock_bars(
                symbol, timeframe, request.start, request.end, request.limit,
                feed=DataFeed(self._config.stock_feed),
                adjustment=Adjustment(self._config.stock_adjustment),
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

    # ─── Daily bar polling ─────────────────────────────────────────────────

    async def _daily_bar_poll_loop(self) -> None:
        """Periodically REST-fetch new bars for any polled subscriptions.

        Batches all polled symbols of the same asset class into one
        multi-symbol call. Dedupe by per-bar_type last-pushed timestamp so
        a freshly published bar (which may appear at the next interval, not
        instantly at market close) only fires ``_handle_data`` once.
        """
        interval = max(30.0, float(self._config.daily_poll_interval_secs))
        while True:
            try:
                await asyncio.sleep(interval)
                if not self._polled_bar_subs:
                    continue
                await self._poll_daily_bars_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._log.warning(f"daily bar poll iteration failed: {exc}")

    async def _poll_daily_bars_once(self) -> None:
        seeded_before = len(self._polled_bar_seeded)
        # Per-cycle counters so we can log a single observable summary line.
        self._poll_cycle_pushed = 0
        # Group polled subscriptions by stream kind so we can batch.
        groups: dict[StreamKind, list[BarType]] = defaultdict(list)
        for bar_type in list(self._polled_bar_subs.keys()):
            sym = bar_type.instrument_id.symbol.value
            if is_option_symbol(sym):
                kind = StreamKind.OPTION
            elif is_crypto_symbol(sym):
                kind = StreamKind.CRYPTO
            else:
                kind = StreamKind.STOCK
            groups[kind].append(bar_type)

        # Look back enough days that a freshly-closed daily bar is included
        # even if the poll runs during a long weekend or holiday.
        from datetime import datetime as _dt
        from datetime import timedelta as _td
        from datetime import timezone as _tz
        start = _dt.now(tz=_tz.utc) - _td(days=5)

        for kind, bar_types in groups.items():
            # Batch into chunks of 100 (Alpaca multi-symbol limit) per
            # bar-spec (different specs need different timeframes).
            by_spec: dict[tuple, list[BarType]] = defaultdict(list)
            for bt in bar_types:
                spec = bt.spec
                by_spec[(spec.step, spec.aggregation)].append(bt)
            for (_step, _agg), bts in by_spec.items():
                symbols = [bt.instrument_id.symbol.value for bt in bts]
                # Take the spec from the first bar_type — all share it.
                timeframe = bar_spec_to_timeframe(bts[0])
                for chunk_start in range(0, len(symbols), 100):
                    chunk = symbols[chunk_start:chunk_start + 100]
                    chunk_bts = bts[chunk_start:chunk_start + 100]
                    try:
                        if kind == StreamKind.CRYPTO:
                            response = await self._http.get_crypto_bars(
                                chunk, timeframe, start, None, None,
                                feed=CryptoFeed(self._config.crypto_feed),
                            )
                        elif kind == StreamKind.OPTION:
                            response = await self._http.get_option_bars(
                                chunk, timeframe, start, None, None,
                            )
                        else:
                            response = await self._http.get_stock_bars(
                                chunk, timeframe, start, None, None,
                                feed=DataFeed(self._config.stock_feed),
                                adjustment=Adjustment(self._config.stock_adjustment),
                            )
                    except Exception as exc:  # noqa: BLE001
                        self._log.warning(f"daily bar poll fetch failed: {exc}")
                        continue
                    self._dispatch_polled_bars(chunk_bts, response)
        seeded_after = len(self._polled_bar_seeded)
        newly_seeded = seeded_after - seeded_before
        if newly_seeded or self._poll_cycle_pushed:
            self._log.info(
                f"daily bar poll: {len(self._polled_bar_subs)} subs, "
                f"seeded={newly_seeded}, pushed={self._poll_cycle_pushed}",
            )

    def _dispatch_polled_bars(self, bar_types: list[BarType], response: Any) -> None:
        data = getattr(response, "data", response)
        if not isinstance(data, dict):
            return
        now_ns = self._clock.timestamp_ns()
        for bar_type in bar_types:
            sym = bar_type.instrument_id.symbol.value
            raw_list = data.get(sym, []) or []
            if not raw_list:
                continue
            instrument = self._polled_bar_subs.get(bar_type)
            if instrument is None:
                continue
            threshold_ns = _bar_completion_ns(bar_type)

            parsed = [parse_bar(raw, bar_type, instrument) for raw in raw_list]
            # Only treat a bar as final after enough time has elapsed since
            # ts_event for Alpaca to have closed it. Without this gate,
            # today's in-progress bar appears with the same ts_event as
            # today's final bar — and the de-dup-on-ts logic would then
            # filter the final bar out as a "duplicate" of itself.
            complete = [b for b in parsed if now_ns - b.ts_event >= threshold_ns]

            if bar_type not in self._polled_bar_seeded:
                if not complete:
                    # Nothing final yet (fresh registration on a weekend
                    # / pre-market). Retry on the next poll cycle.
                    continue
                if self._config.dispatch_first_complete_bar:
                    # Anchor the dedup watermark at SUBSCRIBE time: bars
                    # already final back then are warm-up territory, bars
                    # that completed since must be dispatched below.
                    sub_ns = self._polled_bar_sub_ns.get(bar_type, now_ns)
                    self._polled_bar_last_ts[bar_type] = max(
                        (b.ts_event for b in complete
                         if sub_ns - b.ts_event >= threshold_ns),
                        default=0,
                    )
                    self._polled_bar_seeded.add(bar_type)
                    self._log.debug(
                        f"seeded {bar_type} at subscribe anchor: "
                        f"last_ts={self._polled_bar_last_ts[bar_type]}",
                    )
                    # fall through to the dispatch loop
                else:
                    self._polled_bar_last_ts[bar_type] = max(b.ts_event for b in complete)
                    self._polled_bar_seeded.add(bar_type)
                    self._log.debug(
                        f"seeded {bar_type}: last_ts={self._polled_bar_last_ts[bar_type]}",
                    )
                    continue

            last_ts = self._polled_bar_last_ts.get(bar_type, 0)
            new_pushed = 0
            for bar in complete:
                if bar.ts_event <= last_ts:
                    continue
                self._handle_data(bar)
                last_ts = max(last_ts, bar.ts_event)
                new_pushed += 1
            if new_pushed:
                self._polled_bar_last_ts[bar_type] = last_ts
                self._poll_cycle_pushed += new_pushed
                self._log.info(f"polled {bar_type}: pushed {new_pushed} new bar(s)")

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


_HOUR_NS = 3_600_000_000_000
_DAY_NS = 86_400_000_000_000


def _bar_completion_ns(bar_type: BarType) -> int:
    """Time after ``ts_event`` by which a polled bar can be treated as final.

    Alpaca daily bars use ``ts_event`` = bar's *start* (e.g. 04:00 UTC
    for US equities) but the bar isn't finalized until after market close
    (~20:00 UTC under DST, ~21:00 EST). 16h covers both regimes with a
    margin. Weekly/monthly thresholds are conservative — push happens at
    most one full bar-duration late, which is acceptable for those
    aggregations.
    """
    from nautilus_trader.model.enums import BarAggregation
    spec = bar_type.spec
    if spec.aggregation == BarAggregation.DAY:
        return 16 * _HOUR_NS
    if spec.aggregation == BarAggregation.WEEK:
        return 7 * _DAY_NS
    if spec.aggregation == BarAggregation.MONTH:
        return 31 * _DAY_NS
    return _DAY_NS
