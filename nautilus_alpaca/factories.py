"""Factories for wiring Alpaca live clients into NautilusTrader.

A ``TradingNode`` instantiates the data and execution clients independently,
but Alpaca's REST credentials are the same in both cases. To avoid creating
two ``TradingClient`` instances per node, we keep a per-credential cache:
the first factory call (data or execution) builds the shared
``AlpacaHttpClient`` and the second one reuses it.
"""
from __future__ import annotations

import asyncio

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory

from nautilus_alpaca.common.credentials import load_credentials
from nautilus_alpaca.config import AlpacaDataClientConfig
from nautilus_alpaca.config import AlpacaExecClientConfig
from nautilus_alpaca.data import AlpacaDataClient
from nautilus_alpaca.execution import AlpacaExecutionClient
from nautilus_alpaca.http.client import AlpacaHttpClient
from nautilus_alpaca.providers import AlpacaInstrumentProvider
from nautilus_alpaca.websocket.trading import AlpacaTradingWebSocket


_HTTP_CLIENT_CACHE: dict[tuple[str, str, bool, str | None, bool], AlpacaHttpClient] = {}
_PROVIDER_CACHE: dict[tuple[int, int], AlpacaInstrumentProvider] = {}


def get_alpaca_http_client(
    api_key: str,
    api_secret: str,
    paper: bool,
    url_override: str | None = None,
    data_sandbox: bool = False,
) -> AlpacaHttpClient:
    """Return a shared ``AlpacaHttpClient`` for the given credentials.

    Calling this from both the data and execution factories ensures that a
    single process opens at most one connection pool to Alpaca's REST API
    per credential set.
    """
    key = (api_key, api_secret, paper, url_override, data_sandbox)
    client = _HTTP_CLIENT_CACHE.get(key)
    if client is None:
        client = AlpacaHttpClient(
            api_key=api_key,
            api_secret=api_secret,
            paper=paper,
            url_override=url_override,
            data_sandbox=data_sandbox,
        )
        _HTTP_CLIENT_CACHE[key] = client
    return client


def get_alpaca_instrument_provider(
    http_client: AlpacaHttpClient,
    config: InstrumentProviderConfig,
) -> AlpacaInstrumentProvider:
    """Return a shared ``AlpacaInstrumentProvider`` for the given HTTP client.

    The data and execution clients should both see the same provider so that
    instruments loaded by one are visible to the other.
    """
    key = (id(http_client), id(config))
    provider = _PROVIDER_CACHE.get(key)
    if provider is None:
        provider = AlpacaInstrumentProvider(client=http_client, config=config)
        _PROVIDER_CACHE[key] = provider
    return provider


class AlpacaLiveDataClientFactory(LiveDataClientFactory):
    """Factory for ``AlpacaDataClient`` instances."""

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: AlpacaDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> AlpacaDataClient:
        credentials = load_credentials(
            api_key=config.api_key,
            api_secret=config.api_secret,
            paper=config.paper,
        )
        http_client = get_alpaca_http_client(
            api_key=credentials.api_key,
            api_secret=credentials.api_secret,
            paper=credentials.paper,
            url_override=config.trading_url_override,
            data_sandbox=config.data_sandbox,
        )
        provider = get_alpaca_instrument_provider(
            http_client=http_client,
            config=config.instrument_provider,
        )
        return AlpacaDataClient(
            loop=loop,
            http_client=http_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
        )


class AlpacaLiveExecClientFactory(LiveExecClientFactory):
    """Factory for ``AlpacaExecutionClient`` instances."""

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: AlpacaExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> AlpacaExecutionClient:
        credentials = load_credentials(
            api_key=config.api_key,
            api_secret=config.api_secret,
            paper=config.paper,
        )
        http_client = get_alpaca_http_client(
            api_key=credentials.api_key,
            api_secret=credentials.api_secret,
            paper=credentials.paper,
            url_override=config.trading_url_override,
        )
        provider = get_alpaca_instrument_provider(
            http_client=http_client,
            config=config.instrument_provider,
        )
        trading_ws = AlpacaTradingWebSocket(
            api_key=credentials.api_key,
            api_secret=credentials.api_secret,
            paper=credentials.paper,
            url_override=config.trading_ws_url_override,
        )
        return AlpacaExecutionClient(
            loop=loop,
            http_client=http_client,
            trading_ws=trading_ws,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
        )


__all__ = [
    "AlpacaLiveDataClientFactory",
    "AlpacaLiveExecClientFactory",
    "get_alpaca_http_client",
    "get_alpaca_instrument_provider",
]
