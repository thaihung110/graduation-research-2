from datetime import datetime
from typing import List, Optional


class TradeValidator:
    """Validate individual trade records."""

    PRICE_MIN = 0.00000001
    PRICE_MAX = 1e15
    VOLUME_MIN = 0.00000001
    VOLUME_MAX = 1e15
    TIMESTAMP_MAX_AGE_MS = 31536000000  # 1 year
    TIMESTAMP_MAX_FUTURE_MS = 86400000  # 1 day

    @staticmethod
    def validate(
        price: Optional[float],
        volume: Optional[float],
        timestamp_ms: Optional[int],
        symbol: Optional[str],
        base_currency: Optional[str],
        quote_currency: Optional[str],
    ) -> List[str]:
        """Validate trade record and return list of error codes."""
        errors = []

        # Price validation
        if price is None or price <= 0:
            errors.append("price_invalid")
        elif (
            price < TradeValidator.PRICE_MIN or price > TradeValidator.PRICE_MAX
        ):
            errors.append("price_out_of_range")

        # Volume validation
        if volume is None or volume <= 0:
            errors.append("volume_invalid")
        elif (
            volume < TradeValidator.VOLUME_MIN
            or volume > TradeValidator.VOLUME_MAX
        ):
            errors.append("volume_out_of_range")

        # Timestamp validation
        if timestamp_ms is None or timestamp_ms <= 0:
            errors.append("timestamp_invalid")
        else:
            current_ms = int(datetime.now().timestamp() * 1000)
            if timestamp_ms < (
                current_ms - TradeValidator.TIMESTAMP_MAX_AGE_MS
            ):
                errors.append("timestamp_too_old")
            elif timestamp_ms > (
                current_ms + TradeValidator.TIMESTAMP_MAX_FUTURE_MS
            ):
                errors.append("timestamp_too_future")

        # Symbol validation
        if not symbol or (isinstance(symbol, str) and symbol.strip() == ""):
            errors.append("symbol_empty")
        elif isinstance(symbol, str) and ":" not in symbol:
            errors.append("symbol_invalid_format")

        # Currency validation
        if not base_currency or (
            isinstance(base_currency, str) and base_currency.strip() == ""
        ):
            errors.append("base_currency_empty")
        if not quote_currency or (
            isinstance(quote_currency, str) and quote_currency.strip() == ""
        ):
            errors.append("quote_currency_empty")
        if base_currency == quote_currency:
            errors.append("currencies_same")

        return errors

