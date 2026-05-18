"""Connect to Alpaca's live data feed and print incoming quotes/trades.

Standalone — does not use a NautilusTrader ``TradingNode``. Useful as a
sanity check that credentials and the WebSocket wrapper work.

Prerequisites:
    export ALPACA_API_KEY=...
    export ALPACA_API_SECRET=...
"""
from __future__ import annotations

import asyncio
import signal
from contextlib import suppress

from alpaca.data.enums import DataFeed
from alpaca.data.models import Quote
from alpaca.data.models import Trade

from nautilus_alpaca.common.credentials import load_credentials
from nautilus_alpaca.websocket.data import AlpacaDataWebSocket
from nautilus_alpaca.websocket.data import StreamKind


SYMBOLS = ("AAPL", "MSFT")


async def on_quote(quote: Quote) -> None:
    print(f"QUOTE {quote.symbol} bid={quote.bid_price}x{quote.bid_size} "
          f"ask={quote.ask_price}x{quote.ask_size}")


async def on_trade(trade: Trade) -> None:
    print(f"TRADE {trade.symbol} {trade.size}@{trade.price}")


async def main() -> None:
    credentials = load_credentials(paper=True)
    ws = AlpacaDataWebSocket(
        kind=StreamKind.STOCK,
        api_key=credentials.api_key,
        api_secret=credentials.api_secret,
        stock_feed=DataFeed.IEX,
    )
    ws.subscribe_quotes(on_quote, *SYMBOLS)
    ws.subscribe_trades(on_trade, *SYMBOLS)
    await ws.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    print("Streaming — press Ctrl+C to exit.")
    await stop_event.wait()
    await ws.stop()


if __name__ == "__main__":
    asyncio.run(main())
