"""
Symbol parser utilities for Finnhub data
"""

import re
from typing import Dict, Optional, Tuple


class SymbolParser:
    """Parse and extract information from Finnhub symbols"""

    # Common crypto exchanges
    CRYPTO_EXCHANGES = [
        "BINANCE",
        "COINBASE",
        "KRAKEN",
        "BITFINEX",
        "BITSTAMP",
        "GEMINI",
        "HUOBI",
        "BITTREX",
    ]

    # Common fiat currencies
    FIAT_CURRENCIES = ["USD", "USDT", "USDC", "BUSD", "EUR", "GBP", "JPY"]

    @staticmethod
    def parse_symbol(symbol: str) -> Dict[str, Optional[str]]:
        """
        Parse Finnhub symbol into components

        Args:
            symbol: Finnhub symbol (e.g., "BINANCE:BTCUSDT")

        Returns:
            Dict with exchange, base_currency, quote_currency, and symbol_type
        """
        result = {
            "exchange": None,
            "base_currency": None,
            "quote_currency": None,
            "symbol_type": "unknown",
            "original_symbol": symbol,
        }

        # Check if it's a crypto symbol (contains colon)
        if ":" in symbol:
            parts = symbol.split(":", 1)
            if len(parts) == 2:
                exchange, pair = parts

                # Check if it's a known crypto exchange
                if exchange.upper() in SymbolParser.CRYPTO_EXCHANGES:
                    result["exchange"] = exchange.upper()
                    result["symbol_type"] = "crypto"

                    # Try to extract base and quote currency
                    base, quote = SymbolParser._extract_currencies(pair)
                    result["base_currency"] = base
                    result["quote_currency"] = quote
        else:
            # Stock symbol (no colon)
            result["symbol_type"] = "stock"
            result["base_currency"] = symbol

        return result

    @staticmethod
    def _extract_currencies(pair: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract base and quote currency from trading pair

        Args:
            pair: Trading pair (e.g., "BTCUSDT")

        Returns:
            Tuple of (base_currency, quote_currency)
        """
        pair_upper = pair.upper()

        # Try to find quote currency (usually at the end)
        for fiat in SymbolParser.FIAT_CURRENCIES:
            if pair_upper.endswith(fiat):
                quote = fiat
                base = pair_upper[: -len(fiat)]
                return (base, quote)

        # If no fiat found, try common patterns
        # For BTC pairs
        if pair_upper.endswith("BTC"):
            return (pair_upper[:-3], "BTC")

        # For ETH pairs
        if pair_upper.endswith("ETH"):
            return (pair_upper[:-3], "ETH")

        # Default: return None if can't parse
        return (None, None)

    @staticmethod
    def is_crypto_symbol(symbol: str) -> bool:
        """Check if symbol is a cryptocurrency symbol"""
        if ":" not in symbol:
            return False

        exchange = symbol.split(":", 1)[0].upper()
        return exchange in SymbolParser.CRYPTO_EXCHANGES

    @staticmethod
    def format_symbol_for_kafka_key(symbol: str) -> str:
        """
        Format symbol for use as Kafka partition key

        Args:
            symbol: Original symbol

        Returns:
            Formatted key (e.g., "binance_btcusdt")
        """
        return symbol.lower().replace(":", "_")
