# Silver Layer Schema Design - Crypto Trades

## Overview

Silver layer contains cleaned, validated, and enriched crypto trade data from Bronze layer. This document defines the schema for the `crypto_trades` table in the Silver warehouse.

## Design Principles

1. **Data Quality**: Remove invalid/null records, standardize formats
2. **Business Enrichment**: Add calculated fields (trade value, price changes)
3. **Partitioning**: Optimize for time-based queries
4. **Metadata**: Track data quality metrics and transformations
5. **Simplification**: Remove technical fields (Kafka metadata) not needed for analysis

## Bronze Schema (Input)

From `bronze.crypto_trades_raw`:

| Column                   | Type          | Description                         |
| ------------------------ | ------------- | ----------------------------------- |
| `symbol`                 | string        | Full symbol (e.g., BINANCE:BTCUSDT) |
| `exchange`               | string        | Exchange name (e.g., BINANCE)       |
| `base_currency`          | string        | Base currency (e.g., BTC)           |
| `quote_currency`         | string        | Quote currency (e.g., USDT)         |
| `price`                  | double        | Trade price                         |
| `volume`                 | double        | Trade volume                        |
| `trade_datetime`         | timestamp     | Trade timestamp (converted from ms) |
| `trade_date`             | date          | Trade date (partition column)       |
| `timestamp_ms`           | bigint        | Original timestamp in milliseconds  |
| `conditions`             | array<string> | Trade conditions (optional)         |
| `message_type`           | string        | Message type (always "trade")       |
| `kafka_partition`        | int           | Kafka partition number              |
| `kafka_offset`           | bigint        | Kafka offset                        |
| `ingestion_timestamp`    | timestamp     | When record was ingested            |
| `ingestion_timestamp_ms` | bigint        | Producer ingestion timestamp (ms)   |
| `producer_id`            | string        | Producer instance ID                |
| `kafka_topic`            | string        | Kafka topic name                    |
| `schema_version`         | string        | Schema version                      |

## Silver Schema (Output)

### Table: `silver.crypto_trades`

| Column                       | Type          | Nullable | Description                        | Source/Transformation                                  |
| ---------------------------- | ------------- | -------- | ---------------------------------- | ------------------------------------------------------ |
| **Primary Fields**           |
| `trade_id`                   | string        | NO       | Unique trade identifier            | Generated: `{exchange}_{symbol}_{timestamp_ms}_{hash}` |
| `symbol`                     | string        | NO       | Trading pair symbol                | From Bronze (validated)                                |
| `exchange`                   | string        | NO       | Exchange name                      | From Bronze (uppercase, validated)                     |
| `base_currency`              | string        | NO       | Base currency code                 | From Bronze (uppercase, validated)                     |
| `quote_currency`             | string        | NO       | Quote currency code                | From Bronze (uppercase, validated)                     |
| **Trade Data**               |
| `price`                      | decimal(20,8) | NO       | Trade price                        | From Bronze (validated > 0)                            |
| `volume`                     | decimal(20,8) | NO       | Trade volume                       | From Bronze (validated > 0)                            |
| `trade_value`                | decimal(20,8) | NO       | Trade value (price × volume)       | Calculated: `price * volume`                           |
| `trade_datetime`             | timestamp     | NO       | Trade timestamp (UTC)              | From Bronze (normalized to UTC)                        |
| `trade_date`                 | date          | NO       | Trade date (UTC)                   | Partition column, extracted from trade_datetime        |
| `timestamp_ms`               | bigint        | NO       | Original timestamp in milliseconds | From Bronze                                            |
| `hour_of_day`                | int           | NO       | Hour of day (0-23)                 | Extracted from trade_datetime                          |
| `day_of_week`                | int           | NO       | Day of week (1=Monday, 7=Sunday)   | Extracted from trade_datetime                          |
| **Trade Conditions**         |
| `is_buy`                     | boolean       | YES      | Whether trade is a buy order       | Derived from conditions array                          |
| `is_sell`                    | boolean       | YES      | Whether trade is a sell order      | Derived from conditions array                          |
| `is_maker`                   | boolean       | YES      | Whether trade is maker order       | Derived from conditions array                          |
| `is_taker`                   | boolean       | YES      | Whether trade is taker order       | Derived from conditions array                          |
| `trade_side`                 | string        | YES      | Trade side: 'BUY', 'SELL', or NULL | Derived from conditions                                |
| `conditions`                 | array<string> | YES      | Original trade conditions          | From Bronze (cleaned)                                  |
| **Data Quality**             |
| `is_valid`                   | boolean       | NO       | Whether trade passed validation    | Validation result                                      |
| `validation_errors`          | array<string> | YES      | List of validation errors if any   | Validation details                                     |
| `data_quality_score`         | double        | YES      | Data quality score (0.0-1.0)       | Calculated quality metric                              |
| **Metadata**                 |
| `source_system`              | string        | NO       | Source system identifier           | Constant: 'FINNHUB'                                    |
| `bronze_table`               | string        | NO       | Source bronze table                | Constant: 'crypto_trades_raw'                          |
| `bronze_ingestion_timestamp` | timestamp     | NO       | When record was ingested to Bronze | From Bronze                                            |
| `silver_ingestion_timestamp` | timestamp     | NO       | When record was loaded to Silver   | Current timestamp                                      |
| `silver_ingestion_date`      | date          | NO       | Silver ingestion date              | Partition column                                       |
| `transformation_version`     | string        | NO       | Version of transformation logic    | Constant: '1.0'                                        |

### Partitioning Strategy

**Primary Partition**: `trade_date` (daily partitions)
**Secondary Partition**: `exchange` (exchange-based partitioning)

This enables:

- Efficient time-based queries
- Exchange-specific analysis
- Easy data archival by date
- Parallel processing by exchange

### Indexes/Clustering

- **Clustering**: `(exchange, base_currency, quote_currency, trade_datetime)`
- **Sorting**: `trade_datetime DESC` within each partition

## Data Quality Rules

### Validation Rules

1. **Price Validation**:

   - `price > 0`
   - `price` is not NULL
   - `price` is within reasonable bounds (e.g., > 0.00000001, < 1e15)

2. **Volume Validation**:

   - `volume > 0`
   - `volume` is not NULL
   - `volume` is within reasonable bounds

3. **Timestamp Validation**:

   - `timestamp_ms > 0`
   - `trade_datetime` is not NULL
   - `trade_datetime` is within reasonable range (not too far in past/future)

4. **Symbol Validation**:

   - `symbol` is not NULL or empty
   - `symbol` matches pattern: `{EXCHANGE}:{BASE}{QUOTE}`
   - `exchange`, `base_currency`, `quote_currency` are not NULL

5. **Currency Validation**:
   - `base_currency` and `quote_currency` are valid 3-4 character codes
   - `base_currency != quote_currency`

### Data Quality Score Calculation

```
data_quality_score = (
    (price_valid ? 0.2 : 0) +
    (volume_valid ? 0.2 : 0) +
    (timestamp_valid ? 0.2 : 0) +
    (symbol_valid ? 0.2 : 0) +
    (currency_valid ? 0.2 : 0)
)
```

## Transformations

### 1. Data Cleaning

- **Exchange**: Convert to uppercase, validate against known exchanges
- **Currencies**: Convert to uppercase, validate format
- **Price/Volume**: Round to 8 decimal places, remove scientific notation
- **Timestamp**: Normalize to UTC, validate timezone

### 2. Data Enrichment

- **trade_id**: Generate unique identifier
- **trade_value**: Calculate `price × volume`
- **hour_of_day**: Extract hour from trade_datetime
- **day_of_week**: Extract day of week
- **Trade side flags**: Parse conditions array to determine buy/sell, maker/taker

### 3. Data Quality

- **Validation**: Apply all validation rules
- **Scoring**: Calculate data quality score
- **Error tracking**: Record validation errors

### 4. Metadata

- **Source tracking**: Record bronze table and ingestion time
- **Transformation version**: Track transformation logic version
- **Ingestion timestamp**: Record when data was loaded to Silver

## Example Records

### Bronze Input

```json
{
  "symbol": "BINANCE:BTCUSDT",
  "exchange": "BINANCE",
  "base_currency": "BTC",
  "quote_currency": "USDT",
  "price": 87214.52,
  "volume": 0.00162,
  "trade_datetime": "2025-12-18T15:43:17.721Z",
  "trade_date": "2025-12-18",
  "timestamp_ms": 1766056327534,
  "conditions": null,
  "kafka_partition": 0,
  "kafka_offset": 12345
}
```

### Silver Output

```json
{
  "trade_id": "BINANCE_BINANCE:BTCUSDT_1766056327534_a1b2c3d4",
  "symbol": "BINANCE:BTCUSDT",
  "exchange": "BINANCE",
  "base_currency": "BTC",
  "quote_currency": "USDT",
  "price": 87214.52,
  "volume": 0.00162,
  "trade_value": 141.2875224,
  "trade_datetime": "2025-12-18T15:43:17.721Z",
  "trade_date": "2025-12-18",
  "timestamp_ms": 1766056327534,
  "hour_of_day": 15,
  "day_of_week": 3,
  "is_buy": null,
  "is_sell": null,
  "is_maker": null,
  "is_taker": null,
  "trade_side": null,
  "conditions": null,
  "is_valid": true,
  "validation_errors": [],
  "data_quality_score": 1.0,
  "source_system": "FINNHUB",
  "bronze_table": "crypto_trades_raw",
  "bronze_ingestion_timestamp": "2025-12-18T15:43:18.000Z",
  "silver_ingestion_timestamp": "2025-12-18T16:00:00.000Z",
  "silver_ingestion_date": "2025-12-18",
  "transformation_version": "1.0"
}
```

## Iceberg Table Properties

```sql
CREATE TABLE silver.crypto_trades (
  -- columns as defined above
) USING iceberg
PARTITIONED BY (trade_date, exchange)
TBLPROPERTIES (
  'format-version' = '2',
  'write.target-file-size-bytes' = '134217728',  -- 128MB
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.compression-level' = '6',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '10'
)
```

## Next Steps

1. Create PySpark transformation job
2. Implement validation logic
3. Add data quality scoring
4. Set up partitioning and clustering
5. Create Iceberg table with proper schema
