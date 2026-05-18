# Configuration reference

## Credentials

Credentials are resolved in this order:

1. `api_key` / `api_secret` passed explicitly to the config dataclass.
2. Environment variables.
   - Paper: `ALPACA_API_KEY`, `ALPACA_API_SECRET`
   - Live:  `ALPACA_LIVE_API_KEY`, `ALPACA_LIVE_API_SECRET`

If neither is provided, `load_credentials()` raises `RuntimeError`.

## `AlpacaDataClientConfig`

Extends `nautilus_trader.live.config.LiveDataClientConfig`.

| Field                       | Default        | Notes                                                                 |
| --------------------------- | -------------- | --------------------------------------------------------------------- |
| `api_key`, `api_secret`     | `None`         | If `None`, read from env. See above.                                  |
| `paper`                     | `True`         | Toggle paper vs live trading account.                                 |
| `stock_feed`                | `"iex"`        | One of `iex`, `sip`, `delayed_sip`, `otc`, `boats`, `overnight`.      |
| `crypto_feed`               | `"us"`         | Only `us` is exposed by Alpaca today.                                 |
| `option_feed`               | `"indicative"` | One of `opra`, `indicative`.                                          |
| `use_stock_stream`          | `True`         | Open the stock WebSocket on connect.                                  |
| `use_crypto_stream`         | `True`         | Open the crypto WebSocket on connect.                                 |
| `use_option_stream`         | `False`        | Options off by default to save on the OPRA feed cost.                 |
| `trading_url_override`      | `None`         | Override REST trading URL (mostly for tests).                         |
| `stock_ws_url_override`     | `None`         | Override stock WebSocket URL.                                         |
| `crypto_ws_url_override`    | `None`         | Override crypto WebSocket URL.                                        |
| `option_ws_url_override`    | `None`         | Override option WebSocket URL.                                        |
| `data_sandbox`              | `False`        | Point historical-data REST clients at Alpaca's sandbox.               |

## `AlpacaExecClientConfig`

Extends `nautilus_trader.live.config.LiveExecClientConfig`.

| Field                     | Default | Notes                                            |
| ------------------------- | ------- | ------------------------------------------------ |
| `api_key`, `api_secret`   | `None`  | Same env resolution as data config.              |
| `paper`                   | `True`  | Paper vs live trading.                           |
| `trading_url_override`    | `None`  | Override REST trading URL.                       |
| `trading_ws_url_override` | `None`  | Override `TradingStream` WebSocket URL.          |

## InstrumentProvider filters

Set `instrument_provider=InstrumentProviderConfig(filters={...})` (or pass
`filters=` to `load_all_async`/`load_ids_async`). Recognized keys:

| Key                            | Type                                | Default                      |
| ------------------------------ | ----------------------------------- | ---------------------------- |
| `asset_classes`                | iterable of `"us_equity" \| "crypto" \| "us_option"` | `{"us_equity", "crypto"}` |
| `underlying_symbols`           | `list[str]`                         | required when `us_option` is requested |
| `option_expiration_date_gte`   | `datetime.date`                     | `None`                       |
| `option_expiration_date_lte`   | `datetime.date`                     | `None`                       |
| `include_inactive`             | `bool`                              | `False`                      |

Options are never loaded in bulk across all underlyings — Alpaca exposes
hundreds of thousands of contracts, so you must pass either `load_ids` (for
specific OCC symbols) or `underlying_symbols` (to fetch a chain).

## Shared HTTP client and provider

The data and execution factories share a single `AlpacaHttpClient` per
credential set and a single `AlpacaInstrumentProvider` per
`InstrumentProviderConfig` instance. This means:

- Instruments loaded by the data side are visible to the execution side.
- Both sides hit Alpaca's REST API through one connection pool.

If you instantiate clients manually (not via factories), wire them with the
same `AlpacaHttpClient` and `AlpacaInstrumentProvider` to preserve this
behavior.
