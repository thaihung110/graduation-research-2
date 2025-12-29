"""
Configuration loader for Finnhub Producer
"""

import os
from typing import Any, Dict, List

import yaml


class ConfigLoader:
    """Load and parse configuration from YAML files and environment variables"""

    def __init__(self, config_file: str = "src/config/symbols_config.yaml"):
        self.config_file = config_file
        self.config = self._load_yaml_config()
        self._override_from_env()

    def _load_yaml_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if os.path.exists(self.config_file):
            with open(self.config_file, "r") as f:
                return yaml.safe_load(f)
        return {}

    def _override_from_env(self):
        """Override config with environment variables (for K8s deployment)"""
        # Kafka settings
        if "KAFKA_SERVER" in os.environ:
            self.config.setdefault("kafka", {})["server"] = os.environ[
                "KAFKA_SERVER"
            ]

        if "KAFKA_PORT" in os.environ:
            self.config.setdefault("kafka", {})["port"] = os.environ[
                "KAFKA_PORT"
            ]

        # Finnhub API token
        if "FINNHUB_API_TOKEN" in os.environ:
            self.config.setdefault("finnhub", {})["api_token"] = os.environ[
                "FINNHUB_API_TOKEN"
            ]

        # Symbols override (for K8s)
        if "FINNHUB_STOCKS_TICKERS" in os.environ:
            import ast

            symbols = ast.literal_eval(os.environ["FINNHUB_STOCKS_TICKERS"])
            # Convert to config format
            self.config.setdefault("websocket", {})[
                "symbols_override"
            ] = symbols

    def get_enabled_symbols(self) -> List[str]:
        """Get list of enabled symbols from config"""
        if "websocket" not in self.config:
            return []

        # Check for override (from K8s env)
        if "symbols_override" in self.config["websocket"]:
            return self.config["websocket"]["symbols_override"]

        # Get from YAML config
        symbols = []
        for tier_name, tier_symbols in self.config["websocket"][
            "symbols"
        ].items():
            for symbol_config in tier_symbols:
                if symbol_config.get("enabled", False):
                    symbols.append(symbol_config["symbol"])

        return symbols

    def get_kafka_topic(self, data_type: str = "crypto_trades") -> str:
        """Get Kafka topic name for data type"""
        topics = self.config.get("kafka", {}).get("topics", {})
        return topics.get(data_type, f"finnhub.{data_type}")

    def get_producer_id(self) -> str:
        """Get producer ID"""
        return self.config.get("producer", {}).get(
            "id", "finnhub-producer-unknown"
        )

    def get_schema_version(self) -> str:
        """Get schema version"""
        return self.config.get("producer", {}).get("schema_version", "1.0.0")

    def is_validation_enabled(self) -> bool:
        """Check if symbol validation is enabled"""
        return not self.config.get("validation", {}).get("skip_crypto", True)

    def is_enrichment_enabled(self) -> bool:
        """Check if data enrichment is enabled"""
        return self.config.get("enrichment", {}).get(
            "add_producer_metadata", True
        )
