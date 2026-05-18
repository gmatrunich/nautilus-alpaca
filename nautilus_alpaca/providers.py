"""Instrument provider for the Alpaca venue.

Loads three asset classes via Alpaca's REST API:

- US equities (``TradingClient.get_all_assets`` filtered by ``AssetClass.US_EQUITY``)
- Crypto pairs (``AssetClass.CRYPTO``)
- US option contracts (``TradingClient.get_option_contracts``)

Equities and crypto pairs are returned in a single bulk call. Options total
hundreds of thousands of active contracts across all underlyings, so we never
fetch them in bulk — callers must pass ``load_ids`` or filters that restrict
the result set.

Filters accepted via ``InstrumentProviderConfig.filters`` /
``load_*_async(filters=…)``:

- ``asset_classes``: iterable of ``"us_equity" | "crypto" | "us_option"``.
  Defaults to ``{"us_equity", "crypto"}`` — options are opt-in.
- ``underlying_symbols``: list of underlying tickers to fetch option chains for
  (e.g. ``["AAPL", "SPY"]``). Required if ``us_option`` is requested.
- ``option_expiration_date_gte`` / ``option_expiration_date_lte``: ``date``
  bounds applied to option-contract fetches.
- ``include_inactive``: if ``True``, include ``status=inactive`` assets.
  Defaults to ``False``.
"""
from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from typing import Any

from alpaca.trading.enums import AssetClass as AlpacaAssetClass
from alpaca.trading.enums import AssetStatus as AlpacaAssetStatus
from alpaca.trading.enums import ContractType as AlpacaContractType
from alpaca.trading.models import Asset
from alpaca.trading.models import OptionContract as AlpacaOptionContract
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.requests import GetOptionContractsRequest

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.enums import OptionKind
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.instruments import OptionContract
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from nautilus_alpaca.common.constants import ALPACA_VENUE
from nautilus_alpaca.common.symbols import alpaca_symbol_from_instrument_id
from nautilus_alpaca.common.symbols import is_crypto_symbol
from nautilus_alpaca.common.symbols import is_option_symbol
from nautilus_alpaca.http.client import AlpacaHttpClient


_DEFAULT_PRICE_INCREMENT = Decimal("0.01")
_DEFAULT_PRICE_PRECISION = 2
_DEFAULT_EQUITY_LOT_SIZE = 1
_OPTION_CONTRACT_PAGE_LIMIT = 10_000


class AlpacaInstrumentProvider(InstrumentProvider):
    """Loads Alpaca-traded instruments and exposes them as Nautilus ``Instrument``s."""

    def __init__(
        self,
        client: AlpacaHttpClient,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._client = client

    # ── Public API ─────────────────────────────────────────────────────────

    async def load_all_async(self, filters: dict | None = None) -> None:
        merged = self._merge_filters(filters)
        asset_classes = _resolve_asset_classes(merged)
        include_inactive = bool(merged.get("include_inactive", False))

        if AlpacaAssetClass.US_EQUITY in asset_classes:
            await self._load_equities(include_inactive=include_inactive)

        if AlpacaAssetClass.CRYPTO in asset_classes:
            await self._load_crypto(include_inactive=include_inactive)

        if AlpacaAssetClass.US_OPTION in asset_classes:
            underlyings = merged.get("underlying_symbols")
            if not underlyings:
                self._log.warning(
                    "Option loading requested but no `underlying_symbols` filter set; skipping",
                )
            else:
                await self._load_options(
                    underlyings=list(underlyings),
                    expiration_gte=merged.get("option_expiration_date_gte"),
                    expiration_lte=merged.get("option_expiration_date_lte"),
                )

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        if not instrument_ids:
            return

        for instrument_id in instrument_ids:
            if instrument_id.venue != ALPACA_VENUE:
                self._log.warning(
                    f"Skipping instrument id with non-ALPACA venue: {instrument_id}",
                )
                continue
            symbol = alpaca_symbol_from_instrument_id(instrument_id)
            try:
                if is_option_symbol(symbol):
                    await self._load_option_by_symbol(symbol)
                else:
                    await self._load_asset_by_symbol(symbol)
            except Exception as exc:  # noqa: BLE001
                self._log.error(f"Failed to load instrument {instrument_id}: {exc}")

    async def load_async(
        self,
        instrument_id: InstrumentId,
        filters: dict | None = None,
    ) -> None:
        if self.find(instrument_id) is not None:
            return
        await self.load_ids_async([instrument_id], filters)

    # ── Loading helpers ────────────────────────────────────────────────────

    async def _load_equities(self, *, include_inactive: bool) -> None:
        request = GetAssetsRequest(
            asset_class=AlpacaAssetClass.US_EQUITY,
            status=None if include_inactive else AlpacaAssetStatus.ACTIVE,
        )
        assets = await self._client.get_all_assets(request)
        added = 0
        for asset in assets:
            if not asset.tradable and not include_inactive:
                continue
            instrument = self._equity_from_asset(asset)
            if instrument is not None:
                self.add(instrument)
                added += 1
        self._log.info(f"Loaded {added} US equities")

    async def _load_crypto(self, *, include_inactive: bool) -> None:
        request = GetAssetsRequest(
            asset_class=AlpacaAssetClass.CRYPTO,
            status=None if include_inactive else AlpacaAssetStatus.ACTIVE,
        )
        assets = await self._client.get_all_assets(request)
        added = 0
        for asset in assets:
            if not asset.tradable and not include_inactive:
                continue
            instrument = self._crypto_pair_from_asset(asset)
            if instrument is not None:
                self.add(instrument)
                added += 1
        self._log.info(f"Loaded {added} crypto pairs")

    async def _load_options(
        self,
        *,
        underlyings: list[str],
        expiration_gte: date | None,
        expiration_lte: date | None,
    ) -> None:
        added = 0
        for underlying in underlyings:
            page_token: str | None = None
            while True:
                request = GetOptionContractsRequest(
                    underlying_symbols=[underlying],
                    status=AlpacaAssetStatus.ACTIVE,
                    expiration_date_gte=expiration_gte,
                    expiration_date_lte=expiration_lte,
                    page_token=page_token,
                    limit=_OPTION_CONTRACT_PAGE_LIMIT,
                )
                response = await self._client.get_option_contracts(request)
                contracts = getattr(response, "option_contracts", None) or []
                for contract in contracts:
                    instrument = self._option_from_contract(contract)
                    if instrument is not None:
                        self.add(instrument)
                        added += 1
                page_token = getattr(response, "next_page_token", None)
                if not page_token:
                    break
        self._log.info(f"Loaded {added} option contracts")

    async def _load_asset_by_symbol(self, symbol: str) -> None:
        asset = await self._client.get_asset(symbol)
        if asset.asset_class == AlpacaAssetClass.US_EQUITY:
            instrument = self._equity_from_asset(asset)
        elif asset.asset_class in (AlpacaAssetClass.CRYPTO, AlpacaAssetClass.CRYPTO_PERP):
            instrument = self._crypto_pair_from_asset(asset)
        else:
            self._log.warning(
                f"Unsupported asset class for symbol {symbol}: {asset.asset_class}",
            )
            return
        if instrument is not None:
            self.add(instrument)

    async def _load_option_by_symbol(self, symbol: str) -> None:
        contract = await self._client.get_option_contract(symbol)
        instrument = self._option_from_contract(contract)
        if instrument is not None:
            self.add(instrument)

    # ── Parsing ────────────────────────────────────────────────────────────

    def _equity_from_asset(self, asset: Asset) -> Equity | None:
        if asset.asset_class != AlpacaAssetClass.US_EQUITY:
            return None
        price_increment = (
            Decimal(str(asset.price_increment))
            if asset.price_increment
            else _DEFAULT_PRICE_INCREMENT
        )
        price_precision = max(0, -price_increment.as_tuple().exponent)
        ts = self._now_ns()
        instrument_id = InstrumentId(symbol=Symbol(asset.symbol), venue=ALPACA_VENUE)
        return Equity(
            instrument_id=instrument_id,
            raw_symbol=Symbol(asset.symbol),
            currency=Currency.from_str("USD"),
            price_precision=price_precision,
            price_increment=Price(price_increment, price_precision),
            lot_size=Quantity.from_int(_DEFAULT_EQUITY_LOT_SIZE),
            ts_event=ts,
            ts_init=ts,
            info={
                "alpaca_id": str(asset.id),
                "exchange": asset.exchange.value if asset.exchange else None,
                "name": asset.name,
                "fractionable": asset.fractionable,
                "shortable": asset.shortable,
                "marginable": asset.marginable,
                "easy_to_borrow": asset.easy_to_borrow,
                "tradable": asset.tradable,
                "attributes": list(asset.attributes) if asset.attributes else [],
            },
        )

    def _crypto_pair_from_asset(self, asset: Asset) -> CurrencyPair | None:
        if "/" not in asset.symbol:
            self._log.warning(f"Crypto symbol missing '/': {asset.symbol}")
            return None
        base_code, quote_code = asset.symbol.split("/", 1)
        try:
            base = Currency.from_str(base_code)
            quote = Currency.from_str(quote_code)
        except Exception as exc:  # noqa: BLE001
            self._log.warning(
                f"Unknown currency in pair {asset.symbol}: {exc}",
            )
            return None

        price_increment = (
            Decimal(str(asset.price_increment))
            if asset.price_increment
            else _DEFAULT_PRICE_INCREMENT
        )
        price_precision = max(0, -price_increment.as_tuple().exponent)

        size_increment = (
            Decimal(str(asset.min_trade_increment))
            if asset.min_trade_increment
            else Decimal("0.00000001")
        )
        size_precision = max(0, -size_increment.as_tuple().exponent)

        min_quantity = (
            Quantity(Decimal(str(asset.min_order_size)), size_precision)
            if asset.min_order_size
            else None
        )

        ts = self._now_ns()
        instrument_id = InstrumentId(symbol=Symbol(asset.symbol), venue=ALPACA_VENUE)
        return CurrencyPair(
            instrument_id=instrument_id,
            raw_symbol=Symbol(asset.symbol),
            base_currency=base,
            quote_currency=quote,
            price_precision=price_precision,
            size_precision=size_precision,
            price_increment=Price(price_increment, price_precision),
            size_increment=Quantity(size_increment, size_precision),
            min_quantity=min_quantity,
            ts_event=ts,
            ts_init=ts,
            info={
                "alpaca_id": str(asset.id),
                "exchange": asset.exchange.value if asset.exchange else None,
                "name": asset.name,
                "tradable": asset.tradable,
            },
        )

    def _option_from_contract(self, contract: AlpacaOptionContract) -> OptionContract | None:
        price_precision = _DEFAULT_PRICE_PRECISION
        price_increment = _DEFAULT_PRICE_INCREMENT
        strike = Decimal(str(contract.strike_price))
        multiplier = int(Decimal(contract.size)) if contract.size else 100

        activation = self._date_to_ns(contract.expiration_date, end_of_day=False)
        expiration = self._date_to_ns(contract.expiration_date, end_of_day=True)

        option_kind = (
            OptionKind.CALL if contract.type == AlpacaContractType.CALL else OptionKind.PUT
        )
        ts = self._now_ns()
        instrument_id = InstrumentId(symbol=Symbol(contract.symbol), venue=ALPACA_VENUE)
        try:
            return OptionContract(
                instrument_id=instrument_id,
                raw_symbol=Symbol(contract.symbol),
                asset_class=AssetClass.EQUITY,
                currency=Currency.from_str("USD"),
                price_precision=price_precision,
                price_increment=Price(price_increment, price_precision),
                multiplier=Quantity.from_int(multiplier),
                lot_size=Quantity.from_int(1),
                underlying=contract.underlying_symbol,
                option_kind=option_kind,
                strike_price=Price(strike, price_precision),
                activation_ns=activation,
                expiration_ns=expiration,
                ts_event=ts,
                ts_init=ts,
                info={
                    "alpaca_id": contract.id,
                    "name": contract.name,
                    "root_symbol": contract.root_symbol,
                    "style": contract.style.value if contract.style else None,
                    "size": contract.size,
                    "open_interest": contract.open_interest,
                    "open_interest_date": (
                        contract.open_interest_date.isoformat()
                        if contract.open_interest_date
                        else None
                    ),
                    "close_price": contract.close_price,
                    "close_price_date": (
                        contract.close_price_date.isoformat()
                        if contract.close_price_date
                        else None
                    ),
                    "tradable": contract.tradable,
                },
            )
        except Exception as exc:  # noqa: BLE001
            self._log.error(f"Failed to build OptionContract for {contract.symbol}: {exc}")
            return None

    # ── Misc helpers ───────────────────────────────────────────────────────

    def _merge_filters(self, override: dict | None) -> dict[str, Any]:
        merged: dict[str, Any] = dict(self._filters or {})
        if override:
            merged.update(override)
        return merged

    @staticmethod
    def _now_ns() -> int:
        return dt_to_unix_nanos(datetime.now(tz=timezone.utc))

    @staticmethod
    def _date_to_ns(value: date, *, end_of_day: bool) -> int:
        hour, minute, second = (23, 59, 59) if end_of_day else (0, 0, 0)
        dt = datetime(value.year, value.month, value.day, hour, minute, second, tzinfo=timezone.utc)
        return dt_to_unix_nanos(dt)


# ─── Filter helpers ────────────────────────────────────────────────────────


_ASSET_CLASS_ALIASES: dict[str, AlpacaAssetClass] = {
    "us_equity": AlpacaAssetClass.US_EQUITY,
    "equity": AlpacaAssetClass.US_EQUITY,
    "equities": AlpacaAssetClass.US_EQUITY,
    "crypto": AlpacaAssetClass.CRYPTO,
    "cryptocurrency": AlpacaAssetClass.CRYPTO,
    "us_option": AlpacaAssetClass.US_OPTION,
    "option": AlpacaAssetClass.US_OPTION,
    "options": AlpacaAssetClass.US_OPTION,
}


def _resolve_asset_classes(filters: dict[str, Any]) -> set[AlpacaAssetClass]:
    raw: Iterable[str] | None = filters.get("asset_classes")
    if raw is None:
        return {AlpacaAssetClass.US_EQUITY, AlpacaAssetClass.CRYPTO}
    result: set[AlpacaAssetClass] = set()
    for item in raw:
        key = str(item).lower()
        cls = _ASSET_CLASS_ALIASES.get(key)
        if cls is None:
            raise ValueError(f"Unknown asset class filter: {item!r}")
        result.add(cls)
    return result


__all__ = [
    "AlpacaInstrumentProvider",
    "is_crypto_symbol",
    "is_option_symbol",
]
