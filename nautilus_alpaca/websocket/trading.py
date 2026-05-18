"""Async wrapper around alpaca-py's TradingStream (order/trade updates).

The TradingStream broadcasts ``TradeUpdate`` events on order lifecycle
transitions (new, fill, partial_fill, canceled, replaced, …). We schedule
its ``_run_forever()`` coroutine on the NautilusTrader event loop and
forward parsed events to a single user-supplied async handler.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from collections.abc import Callable

from alpaca.trading.models import TradeUpdate
from alpaca.trading.stream import TradingStream


log = logging.getLogger(__name__)


TradeUpdateHandler = Callable[[TradeUpdate], Awaitable[None]]


class AlpacaTradingWebSocket:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        paper: bool = True,
        url_override: str | None = None,
    ) -> None:
        self._stream = TradingStream(
            api_key=api_key,
            secret_key=api_secret,
            paper=paper,
            url_override=url_override,
            raw_data=False,
        )
        self._task: asyncio.Task | None = None
        self._started = False

    @property
    def is_connected(self) -> bool:
        return self._started and self._task is not None and not self._task.done()

    async def start(self, handler: TradeUpdateHandler) -> None:
        """Register the handler and schedule the stream's run loop."""
        if self._started:
            raise RuntimeError("AlpacaTradingWebSocket already started")
        self._stream.subscribe_trade_updates(handler)
        self._task = asyncio.create_task(
            self._stream._run_forever(),
            name="alpaca-trading-ws",
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
