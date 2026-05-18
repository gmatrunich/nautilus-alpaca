from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import Venue


ALPACA: str = "ALPACA"

ALPACA_VENUE: Venue = Venue(ALPACA)
ALPACA_CLIENT_ID: ClientId = ClientId(ALPACA)

TRADING_LIVE_URL: str = "https://api.alpaca.markets"
TRADING_PAPER_URL: str = "https://paper-api.alpaca.markets"

DATA_STOCK_WS_URL: str = "wss://stream.data.alpaca.markets/v2"
DATA_CRYPTO_WS_URL: str = "wss://stream.data.alpaca.markets/v1beta3/crypto"
DATA_OPTION_WS_URL: str = "wss://stream.data.alpaca.markets/v1beta1"
TRADING_LIVE_WS_URL: str = "wss://api.alpaca.markets/stream"
TRADING_PAPER_WS_URL: str = "wss://paper-api.alpaca.markets/stream"
