"""Async wrapper around alpaca-py's market data WebSocket streams.

Alpaca exposes three separate WebSocket endpoints — one for stocks, one for
crypto, one for options. Each requires its own connection. This module
provides :class:`AlpacaDataWebSocket`, a uniform wrapper that schedules the
underlying ``_run_forever()`` coroutine onto the NautilusTrader event loop.

Subscriptions are added incrementally; the stream itself waits idle until
at least one handler is registered, so we can ``start()`` eagerly.
"""
from __future__ import annotations

import asyncio
import enum
import logging
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any

from alpaca.data.enums import CryptoFeed
from alpaca.data.enums import DataFeed
from alpaca.data.enums import OptionsFeed
from alpaca.data.live.crypto import CryptoDataStream
from alpaca.data.live.option import OptionDataStream
from alpaca.data.live.stock import StockDataStream


log = logging.getLogger(__name__)


class StreamKind(str, enum.Enum):
    STOCK = "stock"
    CRYPTO = "crypto"
    OPTION = "option"


Handler = Callable[[Any], Awaitable[None]]


class AlpacaDataWebSocket:
    """Wrap one of the three Alpaca market-data WebSocket streams."""

    def __init__(
        self,
        kind: StreamKind,
        api_key: str,
        api_secret: str,
        *,
        stock_feed: DataFeed = DataFeed.IEX,
        crypto_feed: CryptoFeed = CryptoFeed.US,
        option_feed: OptionsFeed = OptionsFeed.INDICATIVE,
        url_override: str | None = None,
    ) -> None:
        self._kind = kind
        if kind is StreamKind.STOCK:
            self._stream: Any = StockDataStream(
                api_key=api_key,
                secret_key=api_secret,
                feed=stock_feed,
                url_override=url_override,
                raw_data=False,
            )
        elif kind is StreamKind.CRYPTO:
            self._stream = CryptoDataStream(
                api_key=api_key,
                secret_key=api_secret,
                feed=crypto_feed,
                url_override=url_override,
                raw_data=False,
            )
        elif kind is StreamKind.OPTION:
            self._stream = OptionDataStream(
                api_key=api_key,
                secret_key=api_secret,
                feed=option_feed,
                url_override=url_override,
                raw_data=False,
            )
        else:  # pragma: no cover - exhaustive
            raise ValueError(f"Unknown StreamKind: {kind!r}")

        self._task: asyncio.Task | None = None
        self._started = False

    @property
    def kind(self) -> StreamKind:
        return self._kind

    @property
    def is_connected(self) -> bool:
        return self._started and self._task is not None and not self._task.done()

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._started:
            return
        self._task = asyncio.create_task(
            self._stream._run_forever(),
            name=f"alpaca-data-ws-{self._kind.value}",
        )
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        try:
            await self._stream.stop_ws()
        except Exception as exc:  # noqa: BLE001
            log.warning("stop_ws() raised: %s", exc)
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        try:
            await self._stream.close()
        except Exception as exc:  # noqa: BLE001
            log.warning("close() raised: %s", exc)
        self._started = False
        self._task = None

    # ── Subscriptions ──────────────────────────────────────────────────────

    def subscribe_quotes(self, handler: Handler, *symbols: str) -> None:
        self._stream.subscribe_quotes(handler, *symbols)

    def subscribe_trades(self, handler: Handler, *symbols: str) -> None:
        self._stream.subscribe_trades(handler, *symbols)

    def subscribe_bars(self, handler: Handler, *symbols: str) -> None:
        self._stream.subscribe_bars(handler, *symbols)

    def subscribe_updated_bars(self, handler: Handler, *symbols: str) -> None:
        self._stream.subscribe_updated_bars(handler, *symbols)

    def subscribe_daily_bars(self, handler: Handler, *symbols: str) -> None:
        self._stream.subscribe_daily_bars(handler, *symbols)

    def subscribe_orderbooks(self, handler: Handler, *symbols: str) -> None:
        if self._kind is not StreamKind.CRYPTO:
            raise NotImplementedError("Order books are only available for crypto streams")
        self._stream.subscribe_orderbooks(handler, *symbols)

    def unsubscribe_quotes(self, *symbols: str) -> None:
        self._stream.unsubscribe_quotes(*symbols)

    def unsubscribe_trades(self, *symbols: str) -> None:
        self._stream.unsubscribe_trades(*symbols)

    def unsubscribe_bars(self, *symbols: str) -> None:
        self._stream.unsubscribe_bars(*symbols)

    def unsubscribe_updated_bars(self, *symbols: str) -> None:
        self._stream.unsubscribe_updated_bars(*symbols)

    def unsubscribe_daily_bars(self, *symbols: str) -> None:
        self._stream.unsubscribe_daily_bars(*symbols)

    def unsubscribe_orderbooks(self, *symbols: str) -> None:
        if self._kind is not StreamKind.CRYPTO:
            raise NotImplementedError("Order books are only available for crypto streams")
        self._stream.unsubscribe_orderbooks(*symbols)
