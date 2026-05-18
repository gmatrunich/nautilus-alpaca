# nautilus-alpaca

[NautilusTrader](https://nautilustrader.io/) adapter for [Alpaca](https://alpaca.markets/) —
US equities, crypto, and US options (paper or live trading).

> Status: **alpha**. The adapter targets Alpaca's public REST + WebSocket APIs
> via [`alpaca-py`](https://github.com/alpacahq/alpaca-py) and presents them as
> Nautilus `LiveDataClient` / `LiveExecutionClient` implementations.

## Features

- **Market data (WebSocket)** for stocks, crypto, and options — quotes, trades,
  and 1-minute bars.
- **Historical data (REST)** for the same three asset classes — bars (any
  TimeFrame Alpaca supports), trade ticks, and (stocks/crypto only) quote ticks.
- **Order management** — market, limit, stop, stop-limit, trailing-stop;
  submit, cancel, replace, query.
- **Account streaming** — `TradeUpdate` events are translated into Nautilus
  `OrderAccepted` / `OrderFilled` / etc.
- **InstrumentProvider** that loads US equities, crypto pairs, and option
  contracts (filtered by underlying).
- **Standalone historical loader** for backtesting without spinning up a
  `TradingNode`.

## Installation

```bash
pip install nautilus-alpaca
```

Or from source:

```bash
git clone https://github.com/your-org/nautilus-alpaca
cd nautilus-alpaca
pip install -e ".[dev]"
```

## Quickstart

### 1. Set credentials

Paper:

```bash
export ALPACA_API_KEY=...
export ALPACA_API_SECRET=...
```

Live (separate variables so you don't fat-finger live from a paper run):

```bash
export ALPACA_LIVE_API_KEY=...
export ALPACA_LIVE_API_SECRET=...
```

### 2. Download historical bars

```python
from datetime import datetime, timezone
from nautilus_alpaca import AlpacaHistoricalDataLoader

loader = AlpacaHistoricalDataLoader.from_env(paper=True)
bars = loader.bars_sync(
    bar_type="AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL",
    start=datetime(2024, 1, 3, 14, 30, tzinfo=timezone.utc),
    end=datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc),
)
```

### 3. Run a live strategy on paper

```python
from nautilus_alpaca import (
    ALPACA,
    AlpacaDataClientConfig,
    AlpacaExecClientConfig,
    AlpacaLiveDataClientFactory,
    AlpacaLiveExecClientFactory,
)
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.live.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId

aapl = InstrumentId.from_str("AAPL.ALPACA")
config = TradingNodeConfig(
    data_clients={
        ALPACA: AlpacaDataClientConfig(
            paper=True,
            instrument_provider=InstrumentProviderConfig(load_ids=frozenset([aapl])),
        ),
    },
    exec_clients={
        ALPACA: AlpacaExecClientConfig(
            paper=True,
            instrument_provider=InstrumentProviderConfig(load_ids=frozenset([aapl])),
        ),
    },
)
node = TradingNode(config=config)
node.add_data_client_factory(ALPACA, AlpacaLiveDataClientFactory)
node.add_exec_client_factory(ALPACA, AlpacaLiveExecClientFactory)
node.build()
# node.trader.add_strategy(MyStrategy(...))
node.run()
```

See [`examples/`](examples/) for runnable scripts.

## Symbol format

NautilusTrader instrument IDs are `<RAW>.ALPACA` where `<RAW>` is the
exact symbol Alpaca's API returns:

| Asset class | Example raw symbol     | Nautilus `InstrumentId`          |
| ----------- | ---------------------- | -------------------------------- |
| US equity   | `AAPL`                 | `AAPL.ALPACA`                    |
| Crypto      | `BTC/USD`              | `BTC/USD.ALPACA`                 |
| US option   | `AAPL250620C00150000`  | `AAPL250620C00150000.ALPACA`     |

The 21-character OCC option format is parsed into root, expiration, kind,
and strike by `nautilus_alpaca.common.symbols.parse_occ_symbol`.

## Documentation

- [Configuration reference](docs/configuration.md)
- [Limitations and known gaps](docs/limitations.md)

## License

Apache-2.0.
