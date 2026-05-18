"""Configuration dataclasses for the Alpaca adapter.

These extend NautilusTrader's ``LiveDataClientConfig`` / ``LiveExecClientConfig``
with Alpaca-specific fields (API credentials, data-feed selection, paper-or-
live URL toggle, optional URL overrides for testing).

Credentials default to ``None``: in that case the client factory pulls them
from the environment via :func:`nautilus_alpaca.common.credentials.load_credentials`.
"""
from __future__ import annotations

from nautilus_trader.live.config import LiveDataClientConfig
from nautilus_trader.live.config import LiveExecClientConfig


class AlpacaDataClientConfig(LiveDataClientConfig):
    """Configuration for ``AlpacaDataClient``.

    Parameters
    ----------
    api_key, api_secret : str, optional
        API credentials. If ``None`` they are loaded from ``ALPACA_API_KEY`` /
        ``ALPACA_API_SECRET`` (paper) or ``ALPACA_LIVE_API_KEY`` /
        ``ALPACA_LIVE_API_SECRET`` (live).
    paper : bool, default True
        If True, use Alpaca's paper-trading account; otherwise live.
    stock_feed : str, default "iex"
        Stock data feed. One of ``"iex"``, ``"sip"``, ``"delayed_sip"``, ``"otc"``,
        ``"boats"``, ``"overnight"``.
    crypto_feed : str, default "us"
        Crypto data feed (Alpaca currently exposes only ``"us"``).
    option_feed : str, default "indicative"
        Option data feed. One of ``"opra"`` or ``"indicative"``.
    use_stock_stream, use_crypto_stream, use_option_stream : bool
        If True, the data client maintains a live WebSocket connection for that
        asset class. Default: stocks and crypto on, options off.
    trading_url_override, stock_ws_url_override, crypto_ws_url_override,
    option_ws_url_override : str, optional
        Endpoint overrides — useful for tests or proxying.
    data_sandbox : bool, default False
        If True, point historical-data REST clients at Alpaca's sandbox.
    """

    api_key: str | None = None
    api_secret: str | None = None
    paper: bool = True

    stock_feed: str = "iex"
    crypto_feed: str = "us"
    option_feed: str = "indicative"

    use_stock_stream: bool = True
    use_crypto_stream: bool = True
    use_option_stream: bool = False

    trading_url_override: str | None = None
    stock_ws_url_override: str | None = None
    crypto_ws_url_override: str | None = None
    option_ws_url_override: str | None = None

    data_sandbox: bool = False


class AlpacaExecClientConfig(LiveExecClientConfig):
    """Configuration for ``AlpacaExecutionClient``.

    Parameters
    ----------
    api_key, api_secret : str, optional
        API credentials. If ``None`` they are loaded from environment variables
        (see :class:`AlpacaDataClientConfig`).
    paper : bool, default True
        If True, use Alpaca's paper-trading account.
    trading_url_override : str, optional
        REST endpoint override.
    trading_ws_url_override : str, optional
        Trading-stream WebSocket URL override.
    """

    api_key: str | None = None
    api_secret: str | None = None
    paper: bool = True

    trading_url_override: str | None = None
    trading_ws_url_override: str | None = None
