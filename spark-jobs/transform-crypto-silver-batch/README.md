# Crypto OHLCV Silver Batch Transformation Job

Batch Spark job to aggregate crypto trade data from Bronze to Silver layer with hourly OHLCV (Open-High-Low-Close-Volume) statistics.

## Overview

This job reads raw crypto trades from the Bronze Iceberg table and aggregates them into 1-hour OHLCV candles for technical analysis and visualization.

## Silver Schema

### Table: `silver.silver.crypto_ohlcv_1h`

Hourly OHLCV aggregations with the following columns:

| Column             | Type      | Description                          |
| ------------------ | --------- | ------------------------------------ |
| `symbol`           | STRING    | Trading pair (e.g., BINANCE:BTCUSDT) |
| `exchange`         | STRING    | Exchange name                        |
| `base_currency`    | STRING    | Base currency (BTC)                  |
| `quote_currency`   | STRING    | Quote currency (USDT)                |
| `hour_start`       | TIMESTAMP | Hour window start                    |
| `hour_end`         | TIMESTAMP | Hour window end                      |
| `open_price`       | DOUBLE    | First trade price in hour            |
| `high_price`       | DOUBLE    | Highest trade price                  |
| `low_price`        | DOUBLE    | Lowest trade price                   |
| `close_price`      | DOUBLE    | Last trade price                     |
| `total_volume`     | DOUBLE    | Sum of volumes                       |
| `trade_count`      | BIGINT    | Number of trades                     |
| `avg_price`        | DOUBLE    | Average price                        |
| `vwap`             | DOUBLE    | Volume-weighted average price        |
| `price_change`     | DOUBLE    | Close - Open                         |
| `price_change_pct` | DOUBLE    | Percentage change                    |
| `agg_date`         | DATE      | Partition column                     |

**Partitioning**: `(agg_date, exchange)`

## Architecture

```
Bronze (crypto_trades_raw)
    ↓
Extract (date range filter)
    ↓
Transform (1-hour OHLCV aggregation)
    ↓
Silver (crypto_ohlcv_1h)
```

## Build and Deploy

### Build Docker Image

```bash
chmod +x build-image.sh
./build-image.sh
```

### Deploy to Kubernetes

```bash
kubectl apply -f ../../infra/k8s/compute/applications/spark/silver-layer/jobs/transform-crypto-silver-batch.yaml
```

### Check Job Status

```bash
kubectl get sparkapplications
kubectl logs -f <pod-name>
```

## Usage

The job requires two arguments: start_date and end_date (YYYY-MM-DD format).

Example:

```bash
python src/main.py 2025-12-28 2025-12-29
```

## Configuration

Environment variables (set in Kubernetes manifest):

- `BRONZE_CATALOG_URL`: Lakekeeper catalog URL
- `BRONZE_CLIENT_ID`: OAuth client ID for Bronze
- `BRONZE_CLIENT_SECRET`: OAuth client secret
- `SILVER_CATALOG_URL`: Lakekeeper catalog URL
- `SILVER_CLIENT_ID`: OAuth client ID for Silver
- `SILVER_CLIENT_SECRET`: OAuth client secret
- `KEYCLOAK_TOKEN_ENDPOINT`: Keycloak OAuth endpoint
- `AWS_ACCESS_KEY_ID`: MinIO access key
- `AWS_SECRET_ACCESS_KEY`: MinIO secret key

## Querying Silver Data

```sql
-- Get last 24 hours of BTC/USDT data
SELECT *
FROM silver.silver.crypto_ohlcv_1h
WHERE symbol = 'BINANCE:BTCUSDT'
ORDER BY hour_start DESC
LIMIT 24;

-- Daily price change statistics
SELECT
    agg_date,
    symbol,
    COUNT(*) as hours,
    AVG(price_change_pct) as avg_hourly_change_pct,
    MAX(high_price) as day_high,
    MIN(low_price) as day_low
FROM silver.silver.crypto_ohlcv_1h
WHERE symbol = 'BINANCE:BTCUSDT'
GROUP BY agg_date, symbol
ORDER BY agg_date DESC;
```
