"""Helpers for loading Alpaca API credentials from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


_PAPER_KEY_ENV = "ALPACA_API_KEY"
_PAPER_SECRET_ENV = "ALPACA_API_SECRET"
_LIVE_KEY_ENV = "ALPACA_LIVE_API_KEY"
_LIVE_SECRET_ENV = "ALPACA_LIVE_API_SECRET"


@dataclass(frozen=True)
class AlpacaCredentials:
    api_key: str
    api_secret: str
    paper: bool

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        masked = self.api_key[:4] + "…" if self.api_key else "<empty>"
        return f"AlpacaCredentials(api_key={masked}, paper={self.paper})"


def load_credentials(
    api_key: str | None = None,
    api_secret: str | None = None,
    paper: bool = True,
) -> AlpacaCredentials:
    """Return credentials from explicit args, falling back to env vars.

    Paper credentials are read from ``ALPACA_API_KEY`` / ``ALPACA_API_SECRET``;
    live credentials from ``ALPACA_LIVE_API_KEY`` / ``ALPACA_LIVE_API_SECRET``.
    """
    if api_key is not None and api_secret is not None:
        return AlpacaCredentials(api_key=api_key, api_secret=api_secret, paper=paper)
    if paper:
        key = api_key or os.environ.get(_PAPER_KEY_ENV)
        secret = api_secret or os.environ.get(_PAPER_SECRET_ENV)
    else:
        key = api_key or os.environ.get(_LIVE_KEY_ENV)
        secret = api_secret or os.environ.get(_LIVE_SECRET_ENV)
    if not key or not secret:
        env_pair = (
            f"{_PAPER_KEY_ENV}/{_PAPER_SECRET_ENV}"
            if paper
            else f"{_LIVE_KEY_ENV}/{_LIVE_SECRET_ENV}"
        )
        raise RuntimeError(
            f"Alpaca API credentials missing. Set {env_pair} or pass them explicitly.",
        )
    return AlpacaCredentials(api_key=key, api_secret=secret, paper=paper)
