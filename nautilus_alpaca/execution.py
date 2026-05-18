"""Live execution client for Alpaca.

Submits, cancels, and replaces orders via the REST trading API; consumes
``TradeUpdate`` events from the trading WebSocket and translates them into
NautilusTrader execution events (``OrderAccepted``, ``OrderFilled``, …).

Account state is published on connect and on every fill/withdrawal event.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

from alpaca.common.exceptions import APIError
from alpaca.trading.enums import OrderClass as AlpacaOrderClass
from alpaca.trading.enums import OrderType as AlpacaOrderType
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.enums import TimeInForce as AlpacaTimeInForce
from alpaca.trading.enums import TradeEvent
from alpaca.trading.models import Order as AlpacaOrder
from alpaca.trading.models import Position as AlpacaPosition
from alpaca.trading.models import TradeAccount
from alpaca.trading.models import TradeUpdate
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.requests import ReplaceOrderRequest
from alpaca.trading.requests import StopLimitOrderRequest
from alpaca.trading.requests import StopOrderRequest
from alpaca.trading.requests import TrailingStopOrderRequest

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import QueryOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide as NautilusOrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType as NautilusOrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce as NautilusTimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import Order as NautilusOrder

from nautilus_alpaca.common.constants import ALPACA
from nautilus_alpaca.common.constants import ALPACA_CLIENT_ID
from nautilus_alpaca.common.constants import ALPACA_VENUE
from nautilus_alpaca.common.enums import ALPACA_TO_NAUTILUS_ORDER_SIDE
from nautilus_alpaca.common.enums import ALPACA_TO_NAUTILUS_ORDER_STATUS
from nautilus_alpaca.common.enums import NAUTILUS_TO_ALPACA_ORDER_SIDE
from nautilus_alpaca.common.enums import NAUTILUS_TO_ALPACA_TIF
from nautilus_alpaca.common.symbols import instrument_id_from_alpaca_symbol
from nautilus_alpaca.config import AlpacaExecClientConfig
from nautilus_alpaca.http.client import AlpacaHttpClient
from nautilus_alpaca.providers import AlpacaInstrumentProvider
from nautilus_alpaca.websocket.trading import AlpacaTradingWebSocket


_FILL_EVENTS = (TradeEvent.FILL, TradeEvent.PARTIAL_FILL)


class AlpacaExecutionClient(LiveExecutionClient):
    """Live Nautilus execution client for Alpaca."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        http_client: AlpacaHttpClient,
        trading_ws: AlpacaTradingWebSocket,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: AlpacaInstrumentProvider,
        config: AlpacaExecClientConfig,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ALPACA_CLIENT_ID,
            venue=ALPACA_VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            base_currency=Currency.from_str("USD"),
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )
        self._http = http_client
        self._ws = trading_ws
        self._config = config

    # ─── Lifecycle ─────────────────────────────────────────────────────────

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()
        await self._refresh_account_state()
        await self._ws.start(self._on_trade_update)
        self._log.info("AlpacaExecutionClient connected")

    async def _disconnect(self) -> None:
        await self._ws.stop()
        self._log.info("AlpacaExecutionClient disconnected")

    # ─── Order management ──────────────────────────────────────────────────

    async def _submit_order(self, command: SubmitOrder) -> None:
        order = command.order
        instrument_id = order.instrument_id
        symbol = instrument_id.symbol.value
        ts = self._clock.timestamp_ns()
        try:
            request = _build_order_request(symbol, order)
        except Exception as exc:  # noqa: BLE001
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=instrument_id,
                client_order_id=order.client_order_id,
                reason=f"Failed to build Alpaca order request: {exc}",
                ts_event=ts,
            )
            return

        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=instrument_id,
            client_order_id=order.client_order_id,
            ts_event=ts,
        )

        try:
            alpaca_order = await self._http.submit_order(request)
        except APIError as exc:
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=instrument_id,
                client_order_id=order.client_order_id,
                reason=str(exc),
                ts_event=self._clock.timestamp_ns(),
            )
            return

        self.generate_order_accepted(
            strategy_id=order.strategy_id,
            instrument_id=instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=VenueOrderId(str(alpaca_order.id)),
            ts_event=self._clock.timestamp_ns(),
        )

    async def _cancel_order(self, command: CancelOrder) -> None:
        venue_id = command.venue_order_id
        if venue_id is None:
            self._log.error(
                f"Cannot cancel order {command.client_order_id}: no venue order id",
            )
            return
        try:
            await self._http.cancel_order_by_id(str(venue_id))
        except APIError as exc:
            self.generate_order_cancel_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=venue_id,
                reason=str(exc),
                ts_event=self._clock.timestamp_ns(),
            )

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        try:
            await self._http.cancel_orders()
        except APIError as exc:  # noqa: BLE001
            self._log.error(f"cancel_all_orders failed: {exc}")

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        for cancel in command.cancels:
            await self._cancel_order(cancel)

    async def _modify_order(self, command: ModifyOrder) -> None:
        venue_id = command.venue_order_id
        if venue_id is None:
            self._log.error(
                f"Cannot modify order {command.client_order_id}: no venue order id",
            )
            return
        request_kwargs: dict[str, Any] = {}
        if command.quantity is not None:
            request_kwargs["qty"] = str(command.quantity.as_decimal())
        if command.price is not None:
            request_kwargs["limit_price"] = str(command.price.as_decimal())
        if command.trigger_price is not None:
            request_kwargs["stop_price"] = str(command.trigger_price.as_decimal())
        replace_request = ReplaceOrderRequest(**request_kwargs) if request_kwargs else None
        try:
            await self._http.replace_order_by_id(str(venue_id), replace_request)
        except APIError as exc:
            self.generate_order_modify_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=venue_id,
                reason=str(exc),
                ts_event=self._clock.timestamp_ns(),
            )

    async def _query_order(self, command: QueryOrder) -> None:
        if command.venue_order_id is None:
            self._log.error(
                f"Cannot query order {command.client_order_id}: no venue order id",
            )
            return
        try:
            await self._http.get_order_by_id(str(command.venue_order_id))
        except APIError as exc:  # noqa: BLE001
            self._log.warning(f"query_order failed: {exc}")

    # ─── Reconciliation reports ────────────────────────────────────────────

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        try:
            if command.venue_order_id is not None:
                alpaca_order = await self._http.get_order_by_id(str(command.venue_order_id))
            elif command.client_order_id is not None:
                alpaca_order = await self._http.get_order_by_client_id(
                    command.client_order_id.value,
                )
            else:
                self._log.warning("generate_order_status_report: no order id provided")
                return None
        except APIError as exc:  # noqa: BLE001
            self._log.warning(f"generate_order_status_report failed: {exc}")
            return None
        return self._build_order_status_report(alpaca_order)

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        status = QueryOrderStatus.OPEN if command.open_only else QueryOrderStatus.ALL
        symbols = None
        if command.instrument_id is not None:
            symbols = [command.instrument_id.symbol.value]
        request = GetOrdersRequest(
            status=status,
            after=command.start,
            until=command.end,
            symbols=symbols,
            direction="desc",
            limit=500,
            nested=False,
        )
        try:
            alpaca_orders = await self._http.get_orders(request)
        except APIError as exc:  # noqa: BLE001
            self._log.error(f"generate_order_status_reports failed: {exc}")
            return []
        reports: list[OrderStatusReport] = []
        for alpaca_order in alpaca_orders:
            report = self._build_order_status_report(alpaca_order)
            if report is not None:
                reports.append(report)
        return reports

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        try:
            activities = await self._http.get_activities(
                "FILL",
                after=command.start,
                until=command.end,
                direction="desc",
                page_size=500,
            )
        except APIError as exc:  # noqa: BLE001
            self._log.warning(f"generate_fill_reports failed: {exc}")
            return []

        instrument_filter: str | None = (
            command.instrument_id.symbol.value if command.instrument_id is not None else None
        )
        venue_order_filter: str | None = (
            str(command.venue_order_id) if command.venue_order_id is not None else None
        )

        reports: list[FillReport] = []
        for raw in activities:
            symbol = raw.get("symbol")
            if symbol is None:
                continue
            if instrument_filter is not None and symbol != instrument_filter:
                continue
            order_id = raw.get("order_id")
            if venue_order_filter is not None and order_id != venue_order_filter:
                continue
            report = self._build_fill_report(raw)
            if report is not None:
                reports.append(report)
        return reports

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        try:
            positions = await self._http.get_all_positions()
        except APIError as exc:  # noqa: BLE001
            self._log.error(f"generate_position_status_reports failed: {exc}")
            return []
        reports: list[PositionStatusReport] = []
        for pos in positions:
            if command.instrument_id is not None:
                if pos.symbol != command.instrument_id.symbol.value:
                    continue
            report = self._build_position_status_report(pos)
            if report is not None:
                reports.append(report)
        return reports

    # ─── Trade stream handler ──────────────────────────────────────────────

    async def _on_trade_update(self, update: TradeUpdate) -> None:
        try:
            await self._handle_trade_update(update)
        except Exception as exc:  # noqa: BLE001
            self._log.exception(f"Error processing trade update: {exc}")

    async def _handle_trade_update(self, update: TradeUpdate) -> None:
        order = update.order
        if order.symbol is None:
            self._log.warning(f"TradeUpdate has no symbol: {update.event}")
            return
        instrument_id = self._instrument_id_for(order.symbol)
        if order.client_order_id is None:
            self._log.warning(f"TradeUpdate missing client_order_id: {update.event}")
            return
        client_order_id = ClientOrderId(order.client_order_id)
        venue_order_id = VenueOrderId(str(order.id))
        cache_order = self._cache.order(client_order_id)
        strategy_id = cache_order.strategy_id if cache_order is not None else None
        ts_event = dt_to_unix_nanos(update.timestamp)

        event = update.event if isinstance(update.event, TradeEvent) else TradeEvent(update.event)

        if event in _FILL_EVENTS:
            if update.qty is None or update.price is None:
                self._log.warning(f"Fill event missing qty/price: {update}")
                return
            instrument = self._instrument_provider.find(instrument_id)
            if instrument is None:
                self._log.warning(f"Fill on unknown instrument {instrument_id}")
                return
            last_qty = Quantity(Decimal(str(update.qty)), instrument.size_precision)
            last_px = Price(Decimal(str(update.price)), instrument.price_precision)
            commission = Money(Decimal(0), Currency.from_str("USD"))
            order_side = ALPACA_TO_NAUTILUS_ORDER_SIDE.get(
                order.side,
                NautilusOrderSide.NO_ORDER_SIDE,
            )
            order_type = _ORDER_TYPE_MAP.get(order.type, NautilusOrderType.MARKET)
            self.generate_order_filled(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,
                trade_id=TradeId(
                    str(update.execution_id) if update.execution_id else f"alpaca-{ts_event}",
                ),
                order_side=order_side,
                order_type=order_type,
                last_qty=last_qty,
                last_px=last_px,
                quote_currency=Currency.from_str("USD"),
                commission=commission,
                liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
                ts_event=ts_event,
            )
            # Refresh account state after fills to update balances
            await self._refresh_account_state()
        elif event == TradeEvent.NEW or event == TradeEvent.ACCEPTED:
            if strategy_id is not None:
                self.generate_order_accepted(
                    strategy_id=strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=ts_event,
                )
        elif event == TradeEvent.PENDING_NEW:
            if strategy_id is not None:
                self.generate_order_submitted(
                    strategy_id=strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=client_order_id,
                    ts_event=ts_event,
                )
        elif event == TradeEvent.CANCELED:
            if strategy_id is not None:
                self.generate_order_canceled(
                    strategy_id=strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=ts_event,
                )
        elif event == TradeEvent.EXPIRED:
            if strategy_id is not None:
                self.generate_order_expired(
                    strategy_id=strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=ts_event,
                )
        elif event == TradeEvent.REJECTED:
            if strategy_id is not None:
                self.generate_order_rejected(
                    strategy_id=strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=client_order_id,
                    reason="rejected by venue",
                    ts_event=ts_event,
                )
        elif event == TradeEvent.REPLACED:
            if strategy_id is not None and order.qty is not None:
                instrument = self._instrument_provider.find(instrument_id)
                qty = Quantity(
                    Decimal(order.qty),
                    instrument.size_precision if instrument is not None else 0,
                )
                price = (
                    Price(
                        Decimal(order.limit_price),
                        instrument.price_precision if instrument is not None else 2,
                    )
                    if order.limit_price is not None
                    else None
                )
                trigger = (
                    Price(
                        Decimal(order.stop_price),
                        instrument.price_precision if instrument is not None else 2,
                    )
                    if order.stop_price is not None
                    else None
                )
                self.generate_order_updated(
                    strategy_id=strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=venue_order_id,
                    quantity=qty,
                    price=price,
                    trigger_price=trigger,
                    ts_event=ts_event,
                )
        else:
            self._log.debug(f"Unhandled trade event: {event}")

    # ─── Account state ─────────────────────────────────────────────────────

    async def _refresh_account_state(self) -> None:
        try:
            account: TradeAccount = await self._http.get_account()
        except APIError as exc:  # noqa: BLE001
            self._log.error(f"Failed to fetch account state: {exc}")
            return
        ts = self._clock.timestamp_ns()
        currency = Currency.from_str(account.currency or "USD")
        cash = Decimal(account.cash or "0")
        equity = Decimal(account.equity or "0")
        long_market_value = Decimal(account.long_market_value or "0")
        short_market_value = Decimal(account.short_market_value or "0")
        # locked = equity - cash (i.e. value tied up in positions)
        locked = max(equity - cash, Decimal(0))
        free = cash
        total = equity
        balance = AccountBalance(
            total=Money(total, currency),
            locked=Money(locked, currency),
            free=Money(free, currency),
        )
        account_id = AccountId(f"{ALPACA}-{account.account_number or account.id}")
        self._set_account_id(account_id)
        self.generate_account_state(
            balances=[balance],
            margins=[],
            reported=True,
            ts_event=ts,
            info={
                "buying_power": str(account.buying_power),
                "regt_buying_power": str(account.regt_buying_power)
                if account.regt_buying_power
                else None,
                "long_market_value": str(long_market_value),
                "short_market_value": str(short_market_value),
                "status": str(account.status) if account.status else None,
            },
        )

    # ─── Helpers ───────────────────────────────────────────────────────────

    def _instrument_id_for(self, symbol: str) -> InstrumentId:
        return instrument_id_from_alpaca_symbol(symbol)

    def _build_order_status_report(
        self,
        alpaca_order: AlpacaOrder,
    ) -> OrderStatusReport | None:
        if alpaca_order.symbol is None:
            return None
        instrument_id = self._instrument_id_for(alpaca_order.symbol)
        instrument = self._instrument_provider.find(instrument_id)
        price_precision = instrument.price_precision if instrument is not None else 2
        size_precision = instrument.size_precision if instrument is not None else 0

        order_side = ALPACA_TO_NAUTILUS_ORDER_SIDE.get(
            alpaca_order.side,
            NautilusOrderSide.NO_ORDER_SIDE,
        )
        order_type = _ALPACA_ORDER_TYPE_TO_NAUTILUS.get(
            alpaca_order.type if alpaca_order.type is not None else AlpacaOrderType.MARKET,
            NautilusOrderType.MARKET,
        )
        tif = _ALPACA_TIF_TO_NAUTILUS.get(
            alpaca_order.time_in_force,
            NautilusTimeInForce.DAY,
        )
        order_status = ALPACA_TO_NAUTILUS_ORDER_STATUS.get(
            alpaca_order.status,
            OrderStatus.ACCEPTED,
        )
        qty = Quantity(
            Decimal(str(alpaca_order.qty)) if alpaca_order.qty else Decimal(0),
            size_precision,
        )
        filled_qty = Quantity(
            Decimal(str(alpaca_order.filled_qty)) if alpaca_order.filled_qty else Decimal(0),
            size_precision,
        )
        ts_accepted = (
            dt_to_unix_nanos(alpaca_order.submitted_at)
            if alpaca_order.submitted_at
            else dt_to_unix_nanos(alpaca_order.created_at)
        )
        ts_last = dt_to_unix_nanos(alpaca_order.updated_at or alpaca_order.created_at)
        ts_init = self._clock.timestamp_ns()
        client_order_id = (
            ClientOrderId(alpaca_order.client_order_id)
            if alpaca_order.client_order_id
            else None
        )

        kwargs: dict[str, Any] = {
            "account_id": self.account_id or self._fallback_account_id(),
            "instrument_id": instrument_id,
            "venue_order_id": VenueOrderId(str(alpaca_order.id)),
            "order_side": order_side,
            "order_type": order_type,
            "time_in_force": tif,
            "order_status": order_status,
            "quantity": qty,
            "filled_qty": filled_qty,
            "report_id": UUID4(),
            "ts_accepted": ts_accepted,
            "ts_last": ts_last,
            "ts_init": ts_init,
        }
        if client_order_id is not None:
            kwargs["client_order_id"] = client_order_id
        if alpaca_order.limit_price is not None:
            kwargs["price"] = Price(Decimal(str(alpaca_order.limit_price)), price_precision)
        if alpaca_order.stop_price is not None:
            kwargs["trigger_price"] = Price(
                Decimal(str(alpaca_order.stop_price)),
                price_precision,
            )
        if alpaca_order.filled_avg_price is not None:
            kwargs["avg_px"] = Decimal(str(alpaca_order.filled_avg_price))
        return OrderStatusReport(**kwargs)

    def _build_fill_report(self, raw: dict[str, Any]) -> FillReport | None:
        symbol = raw.get("symbol")
        order_id = raw.get("order_id")
        if symbol is None or order_id is None:
            return None
        try:
            qty_raw = raw.get("qty")
            px_raw = raw.get("price")
            if qty_raw is None or px_raw is None:
                return None
            instrument_id = self._instrument_id_for(symbol)
            instrument = self._instrument_provider.find(instrument_id)
            size_precision = instrument.size_precision if instrument is not None else 0
            price_precision = instrument.price_precision if instrument is not None else 2

            side_raw = (raw.get("side") or "").lower()
            order_side = (
                NautilusOrderSide.BUY if side_raw == "buy"
                else NautilusOrderSide.SELL if side_raw == "sell"
                else NautilusOrderSide.NO_ORDER_SIDE
            )
            from datetime import datetime as _dt
            transaction_time = raw.get("transaction_time")
            ts_event = (
                dt_to_unix_nanos(_dt.fromisoformat(transaction_time.replace("Z", "+00:00")))
                if isinstance(transaction_time, str)
                else self._clock.timestamp_ns()
            )
            return FillReport(
                account_id=self.account_id or self._fallback_account_id(),
                instrument_id=instrument_id,
                venue_order_id=VenueOrderId(str(order_id)),
                trade_id=TradeId(str(raw.get("id", f"alpaca-fill-{ts_event}"))),
                order_side=order_side,
                last_qty=Quantity(Decimal(str(qty_raw)), size_precision),
                last_px=Price(Decimal(str(px_raw)), price_precision),
                commission=Money(Decimal(0), Currency.from_str("USD")),
                liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
                report_id=UUID4(),
                ts_event=ts_event,
                ts_init=self._clock.timestamp_ns(),
            )
        except Exception as exc:  # noqa: BLE001
            self._log.warning(f"failed to build FillReport from {raw}: {exc}")
            return None

    def _build_position_status_report(
        self,
        pos: AlpacaPosition,
    ) -> PositionStatusReport | None:
        if pos.symbol is None:
            return None
        instrument_id = self._instrument_id_for(pos.symbol)
        instrument = self._instrument_provider.find(instrument_id)
        size_precision = instrument.size_precision if instrument is not None else 0
        try:
            qty_decimal = Decimal(str(pos.qty)) if pos.qty is not None else Decimal(0)
        except Exception:  # noqa: BLE001
            qty_decimal = Decimal(0)
        position_side = (
            PositionSide.LONG if qty_decimal > 0
            else PositionSide.SHORT if qty_decimal < 0
            else PositionSide.FLAT
        )
        avg_px = (
            Decimal(str(pos.avg_entry_price))
            if pos.avg_entry_price is not None
            else None
        )
        ts = self._clock.timestamp_ns()
        kwargs: dict[str, Any] = {
            "account_id": self.account_id or self._fallback_account_id(),
            "instrument_id": instrument_id,
            "position_side": position_side,
            "quantity": Quantity(abs(qty_decimal), size_precision),
            "report_id": UUID4(),
            "ts_last": ts,
            "ts_init": ts,
        }
        if avg_px is not None:
            kwargs["avg_px_open"] = avg_px
        return PositionStatusReport(**kwargs)

    def _fallback_account_id(self) -> AccountId:
        return AccountId(f"{ALPACA}-PENDING")


# ─── Order request builder ────────────────────────────────────────────────


_ORDER_TYPE_MAP: dict[AlpacaOrderType, NautilusOrderType] = {
    AlpacaOrderType.MARKET: NautilusOrderType.MARKET,
    AlpacaOrderType.LIMIT: NautilusOrderType.LIMIT,
    AlpacaOrderType.STOP: NautilusOrderType.STOP_MARKET,
    AlpacaOrderType.STOP_LIMIT: NautilusOrderType.STOP_LIMIT,
    AlpacaOrderType.TRAILING_STOP: NautilusOrderType.TRAILING_STOP_MARKET,
}

_ALPACA_ORDER_TYPE_TO_NAUTILUS = _ORDER_TYPE_MAP

_ALPACA_TIF_TO_NAUTILUS: dict[AlpacaTimeInForce, NautilusTimeInForce] = {
    AlpacaTimeInForce.DAY: NautilusTimeInForce.DAY,
    AlpacaTimeInForce.GTC: NautilusTimeInForce.GTC,
    AlpacaTimeInForce.IOC: NautilusTimeInForce.IOC,
    AlpacaTimeInForce.FOK: NautilusTimeInForce.FOK,
    AlpacaTimeInForce.OPG: NautilusTimeInForce.AT_THE_OPEN,
    AlpacaTimeInForce.CLS: NautilusTimeInForce.AT_THE_CLOSE,
}


def _build_order_request(symbol: str, order: NautilusOrder):
    side = NAUTILUS_TO_ALPACA_ORDER_SIDE.get(order.side)
    if side is None:
        raise ValueError(f"Unsupported order side: {order.side}")
    tif = NAUTILUS_TO_ALPACA_TIF.get(order.time_in_force)
    if tif is None:
        raise ValueError(f"Unsupported time-in-force: {order.time_in_force}")

    qty = str(order.quantity.as_decimal())
    common: dict[str, Any] = {
        "symbol": symbol,
        "qty": qty,
        "side": side,
        "time_in_force": tif,
        "client_order_id": order.client_order_id.value,
        "order_class": AlpacaOrderClass.SIMPLE,
    }

    if order.order_type == NautilusOrderType.MARKET:
        return MarketOrderRequest(**common)
    if order.order_type == NautilusOrderType.LIMIT:
        return LimitOrderRequest(
            **common,
            limit_price=float(order.price.as_decimal()),
        )
    if order.order_type == NautilusOrderType.STOP_MARKET:
        return StopOrderRequest(
            **common,
            stop_price=float(order.trigger_price.as_decimal()),
        )
    if order.order_type == NautilusOrderType.STOP_LIMIT:
        return StopLimitOrderRequest(
            **common,
            stop_price=float(order.trigger_price.as_decimal()),
            limit_price=float(order.price.as_decimal()),
        )
    if order.order_type == NautilusOrderType.TRAILING_STOP_MARKET:
        kwargs = dict(common)
        if order.trailing_offset is not None and order.trailing_offset_type is not None:
            from nautilus_trader.model.enums import TrailingOffsetType
            if order.trailing_offset_type == TrailingOffsetType.PRICE:
                kwargs["trail_price"] = float(order.trailing_offset)
            else:
                kwargs["trail_percent"] = float(order.trailing_offset)
        return TrailingStopOrderRequest(**kwargs)
    raise ValueError(f"Unsupported order type: {order.order_type}")


