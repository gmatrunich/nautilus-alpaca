"""Download 1-minute bars for AAPL between two dates and print the first five.

Prerequisites:
    export ALPACA_API_KEY=...
    export ALPACA_API_SECRET=...
"""
from __future__ import annotations

from datetime import datetime
from datetime import timezone

from nautilus_alpaca import AlpacaHistoricalDataLoader


def main() -> None:
    loader = AlpacaHistoricalDataLoader.from_env(paper=True)
    bars = loader.bars_sync(
        bar_type="AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL",
        start=datetime(2024, 1, 3, 14, 30, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc),
        limit=5,
    )
    print(f"Downloaded {len(bars)} bars")
    for bar in bars:
        print(bar)


if __name__ == "__main__":
    main()
