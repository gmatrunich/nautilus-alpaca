"""Run a tiny strategy on Alpaca paper-trading via NautilusTrader.

The strategy subscribes to AAPL quote ticks and submits a single 1-share
market buy after receiving the first quote, then stops.

Prerequisites:
    export ALPACA_API_KEY=...
    export ALPACA_API_SECRET=...
"""
from __future__ import annotations

from decimal import Decimal

from nautilus_trader.common.config import InstrumentProviderConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.live.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig

from nautilus_alpaca import ALPACA
from nautilus_alpaca import AlpacaDataClientConfig
from nautilus_alpaca import AlpacaExecClientConfig
from nautilus_alpaca import AlpacaLiveDataClientFactory
from nautilus_alpaca import AlpacaLiveExecClientFactory


class FirstQuoteBuyConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    qty: Decimal = Decimal("1")


class FirstQuoteBuy(Strategy):
    def __init__(self, config: FirstQuoteBuyConfig) -> None:
        super().__init__(config)
        self._fired = False

    def on_start(self) -> None:
        self.subscribe_quote_ticks(self.config.instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        if self._fired:
            return
        self._fired = True
        instrument = self.cache.instrument(self.config.instrument_id)
        if instrument is None:
            self.log.error(f"No instrument for {self.config.instrument_id}")
            return
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity(self.config.qty, instrument.size_precision),
            time_in_force=TimeInForce.DAY,
        )
        self.submit_order(order)
        self.log.info(f"Submitted: {order}")


def main() -> None:
    instrument_id = InstrumentId.from_str("AAPL.ALPACA")
    config = TradingNodeConfig(
        logging=LoggingConfig(log_level="INFO"),
        data_clients={
            ALPACA: AlpacaDataClientConfig(
                paper=True,
                instrument_provider=InstrumentProviderConfig(
                    load_ids=frozenset([instrument_id]),
                ),
            ),
        },
        exec_clients={
            ALPACA: AlpacaExecClientConfig(
                paper=True,
                instrument_provider=InstrumentProviderConfig(
                    load_ids=frozenset([instrument_id]),
                ),
            ),
        },
        strategies=[],
    )
    node = TradingNode(config=config)
    node.add_data_client_factory(ALPACA, AlpacaLiveDataClientFactory)
    node.add_exec_client_factory(ALPACA, AlpacaLiveExecClientFactory)
    node.build()
    node.trader.add_strategy(
        FirstQuoteBuy(FirstQuoteBuyConfig(instrument_id=instrument_id)),
    )
    try:
        node.run()
    finally:
        node.dispose()


if __name__ == "__main__":
    main()
