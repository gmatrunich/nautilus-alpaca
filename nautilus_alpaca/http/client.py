"""Async facade over alpaca-py's synchronous REST clients.

``alpaca-py`` exposes blocking REST methods. NautilusTrader runs adapter
callbacks inside an asyncio event loop, so we run every blocking call on
the loop's default executor via :func:`asyncio.to_thread`.

A single :class:`AlpacaHttpClient` instance owns the trading client (paper
or live) plus historical-data clients for stocks, crypto, and options.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any
from uuid import UUID

from alpaca.data.enums import CryptoFeed
from alpaca.data.enums import DataFeed
from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.requests import CryptoQuoteRequest
from alpaca.data.requests import CryptoTradesRequest
from alpaca.data.requests import OptionBarsRequest
from alpaca.data.requests import OptionTradesRequest
from alpaca.data.requests import StockBarsRequest
from alpaca.data.requests import StockQuotesRequest
from alpaca.data.requests import StockTradesRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.models import Asset
from alpaca.trading.models import Clock
from alpaca.trading.models import Order
from alpaca.trading.models import Position
from alpaca.trading.models import TradeAccount
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.requests import GetOptionContractsRequest
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.requests import OrderRequest
from alpaca.trading.requests import ReplaceOrderRequest


class AlpacaHttpClient:
    """Async wrapper around alpaca-py REST clients."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        paper: bool = True,
        url_override: str | None = None,
        data_sandbox: bool = False,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._paper = paper
        self._trading = TradingClient(
            api_key=api_key,
            secret_key=api_secret,
            paper=paper,
            url_override=url_override,
            raw_data=False,
        )
        self._stock_data = StockHistoricalDataClient(
            api_key=api_key,
            secret_key=api_secret,
            raw_data=False,
            sandbox=data_sandbox,
        )
        self._crypto_data = CryptoHistoricalDataClient(
            api_key=api_key,
            secret_key=api_secret,
            raw_data=False,
            sandbox=data_sandbox,
        )
        self._option_data = OptionHistoricalDataClient(
            api_key=api_key,
            secret_key=api_secret,
            raw_data=False,
            sandbox=data_sandbox,
        )

    @property
    def paper(self) -> bool:
        return self._paper

    @property
    def api_key(self) -> str:
        return self._api_key

    @property
    def api_secret(self) -> str:
        return self._api_secret

    # ── Account ────────────────────────────────────────────────────────────

    async def get_account(self) -> TradeAccount:
        return await asyncio.to_thread(self._trading.get_account)

    async def get_clock(self) -> Clock:
        return await asyncio.to_thread(self._trading.get_clock)

    # ── Assets / instruments ───────────────────────────────────────────────

    async def get_all_assets(self, filter: GetAssetsRequest | None = None) -> list[Asset]:
        return await asyncio.to_thread(self._trading.get_all_assets, filter)

    async def get_asset(self, symbol_or_id: str | UUID) -> Asset:
        return await asyncio.to_thread(self._trading.get_asset, symbol_or_id)

    async def get_option_contracts(self, filter: GetOptionContractsRequest) -> Any:
        return await asyncio.to_thread(self._trading.get_option_contracts, filter)

    async def get_option_contract(self, symbol_or_id: str | UUID) -> Any:
        return await asyncio.to_thread(self._trading.get_option_contract, symbol_or_id)

    # ── Orders ─────────────────────────────────────────────────────────────

    async def submit_order(self, order_data: OrderRequest) -> Order:
        return await asyncio.to_thread(self._trading.submit_order, order_data)

    async def get_orders(self, filter: GetOrdersRequest | None = None) -> list[Order]:
        return await asyncio.to_thread(self._trading.get_orders, filter)

    async def get_order_by_id(self, order_id: str | UUID) -> Order:
        return await asyncio.to_thread(self._trading.get_order_by_id, order_id)

    async def get_order_by_client_id(self, client_id: str) -> Order:
        return await asyncio.to_thread(self._trading.get_order_by_client_id, client_id)

    async def cancel_order_by_id(self, order_id: str | UUID) -> None:
        await asyncio.to_thread(self._trading.cancel_order_by_id, order_id)

    async def cancel_orders(self) -> list[Any]:
        return await asyncio.to_thread(self._trading.cancel_orders)

    async def replace_order_by_id(
        self,
        order_id: str | UUID,
        order_data: ReplaceOrderRequest | None = None,
    ) -> Order:
        return await asyncio.to_thread(
            self._trading.replace_order_by_id,
            order_id,
            order_data,
        )

    async def get_activities(
        self,
        activity_type: str = "FILL",
        *,
        after: datetime | None = None,
        until: datetime | None = None,
        direction: str = "desc",
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Fetch raw account-activity records (fills, dividends, etc.).

        ``alpaca-py`` does not type these, so we hit the REST endpoint
        directly. Returns the JSON list as-is. ``activity_type="FILL"``
        is what reconciliation cares about.
        """
        params: dict[str, Any] = {
            "direction": direction,
            "page_size": page_size,
        }
        if after is not None:
            params["after"] = after.isoformat()
        if until is not None:
            params["until"] = until.isoformat()
        path = f"/account/activities/{activity_type}"
        return await asyncio.to_thread(self._trading.get, path, params)

    # ── Positions ──────────────────────────────────────────────────────────

    async def get_all_positions(self) -> list[Position]:
        return await asyncio.to_thread(self._trading.get_all_positions)

    async def get_open_position(self, symbol_or_id: str | UUID) -> Position:
        return await asyncio.to_thread(self._trading.get_open_position, symbol_or_id)

    # ── Historical stock data ──────────────────────────────────────────────

    async def get_stock_bars(
        self,
        symbol_or_symbols: str | list[str],
        timeframe: TimeFrame,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        feed: DataFeed | None = None,
    ) -> Any:
        request = StockBarsRequest(
            symbol_or_symbols=symbol_or_symbols,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
            feed=feed,
        )
        return await asyncio.to_thread(self._stock_data.get_stock_bars, request)

    async def get_stock_quotes(
        self,
        symbol_or_symbols: str | list[str],
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        feed: DataFeed | None = None,
    ) -> Any:
        request = StockQuotesRequest(
            symbol_or_symbols=symbol_or_symbols,
            start=start,
            end=end,
            limit=limit,
            feed=feed,
        )
        return await asyncio.to_thread(self._stock_data.get_stock_quotes, request)

    async def get_stock_trades(
        self,
        symbol_or_symbols: str | list[str],
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        feed: DataFeed | None = None,
    ) -> Any:
        request = StockTradesRequest(
            symbol_or_symbols=symbol_or_symbols,
            start=start,
            end=end,
            limit=limit,
            feed=feed,
        )
        return await asyncio.to_thread(self._stock_data.get_stock_trades, request)

    # ── Historical crypto data ─────────────────────────────────────────────

    async def get_crypto_bars(
        self,
        symbol_or_symbols: str | list[str],
        timeframe: TimeFrame,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        feed: CryptoFeed = CryptoFeed.US,
    ) -> Any:
        request = CryptoBarsRequest(
            symbol_or_symbols=symbol_or_symbols,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        )
        return await asyncio.to_thread(self._crypto_data.get_crypto_bars, request, feed)

    async def get_crypto_quotes(
        self,
        symbol_or_symbols: str | list[str],
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        feed: CryptoFeed = CryptoFeed.US,
    ) -> Any:
        request = CryptoQuoteRequest(
            symbol_or_symbols=symbol_or_symbols,
            start=start,
            end=end,
            limit=limit,
        )
        return await asyncio.to_thread(self._crypto_data.get_crypto_quotes, request, feed)

    async def get_crypto_trades(
        self,
        symbol_or_symbols: str | list[str],
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        feed: CryptoFeed = CryptoFeed.US,
    ) -> Any:
        request = CryptoTradesRequest(
            symbol_or_symbols=symbol_or_symbols,
            start=start,
            end=end,
            limit=limit,
        )
        return await asyncio.to_thread(self._crypto_data.get_crypto_trades, request, feed)

    # ── Historical options data ────────────────────────────────────────────

    async def get_option_bars(
        self,
        symbol_or_symbols: str | list[str],
        timeframe: TimeFrame,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Any:
        request = OptionBarsRequest(
            symbol_or_symbols=symbol_or_symbols,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        )
        return await asyncio.to_thread(self._option_data.get_option_bars, request)

    async def get_option_trades(
        self,
        symbol_or_symbols: str | list[str],
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Any:
        request = OptionTradesRequest(
            symbol_or_symbols=symbol_or_symbols,
            start=start,
            end=end,
            limit=limit,
        )
        return await asyncio.to_thread(self._option_data.get_option_trades, request)
