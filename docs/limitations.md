# Limitations and known gaps

## Market data

- **Live bars are 1-minute only.** Alpaca streams 1-minute aggregates over
  WebSocket. For other periods, subscribe to trades and aggregate via
  Nautilus's internal aggregator (`aggregation_source=INTERNAL`).
- **Historical option quotes are not exposed** by Alpaca's REST API in the
  same shape as stocks/crypto, so `quote_ticks()` raises
  `NotImplementedError` for OCC symbols.
- **Order books / L2 depth** are available for crypto only and not yet
  wired through this adapter.
- **Aggressor side** on trade ticks is reported as `NO_AGGRESSOR` — Alpaca
  does not publish the maker/taker distinction in the same form.

## Execution

- **Order classes other than `simple` are not yet supported.** Brackets,
  OCO, and OTO will be added in a future release.
- **Position reconciliation is best-effort.** On connect we publish account
  balances; positions are not currently reconciled against Nautilus's cache
  on startup (Nautilus's execution engine will run its own reconciliation
  pass against open orders).
- **`mleg` (multi-leg) option orders** are not supported.
- **Trailing-stop modifications** can only adjust `qty` / `limit_price` /
  `stop_price`. Trailing offsets cannot be modified after submission.

## Account

- `AccountType` is always reported as `MARGIN`. Alpaca's API exposes whether
  the account is cash or margin in the account record; this is read on
  connect but not surfaced as Nautilus's `AccountType` (which is fixed at
  client-creation time).
- `base_currency` is fixed to USD. Crypto sub-accounts denominated in
  other currencies are not yet supported.
- Commissions on fills are reported as `Money(0, USD)`. Alpaca's API does
  not surface per-fill commission in the `TradeUpdate` event.

## Symbol / instrument coverage

- Crypto perp (`CRYPTO_PERP`) assets are parsed as `CurrencyPair` like spot
  pairs — funding/perp metadata is preserved in `info` but not modeled as
  Nautilus `CryptoPerpetual`.
- Option contracts are emitted with `AssetClass.EQUITY` (Nautilus has no
  dedicated US-equity-option asset class). The exchange / OCC root is
  available via the instrument's `info` dict.

## Roadmap

- Bracket / OCO / OTO order classes
- L2 order books for crypto
- Multi-leg option orders
- Per-fill commission reporting (once Alpaca surfaces it)
- Crypto-perp first-class support
