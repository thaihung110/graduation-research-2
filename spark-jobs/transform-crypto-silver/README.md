# Crypto Trades Silver Transformation Job

PySpark application to transform crypto trades from Bronze to Silver layer.

## Overview

This job:

1. Reads cleaned data from Iceberg table `bronze.crypto_trades_raw`
2. Validates and enriches the data
3. Applies data quality checks and scoring
4. Transforms to Silver schema with business fields
5. Loads data into Iceberg table `silver.crypto_trades`

## Files

- `transform_crypto_silver.py` - Main PySpark application
- `Dockerfile` - Docker image definition
- `build-image.sh` - Script to build and push Docker image to Docker Hub
- `SCHEMA_DESIGN.md` - Detailed Silver schema design document

**Docker Image**: `hungvt0110/transform-crypto-silver`

## Data Flow

```
Bronze Table (bronze.crypto_trades_raw)
    ↓ (Read via bronze catalog)
Transform & Validate
    ↓ (Data quality checks)
Enrich with Business Fields
    ↓ (Calculate metrics)
Silver Table (silver.crypto_trades)
    ↓ (Write via silver catalog)
```

## Dual Catalog Configuration

This job uses **two separate Iceberg catalogs**:

1. **Bronze Catalog** (`bronze`): Read from Bronze warehouse
2. **Silver Catalog** (`silver`): Write to Silver warehouse

Both catalogs connect to the same lakekeeper instance but use different warehouses.

## Input Schema (Bronze)

See `SCHEMA_DESIGN.md` for detailed Bronze schema.

## Output Schema (Silver)

See `SCHEMA_DESIGN.md` for detailed Silver schema.

Key additions:

- `trade_id`: Unique identifier
- `trade_value`: Calculated `price × volume`
- `hour_of_day`, `day_of_day`: Time-based fields
- `is_buy`, `is_sell`, `is_maker`, `is_taker`: Trade condition flags
- `is_valid`, `validation_errors`, `data_quality_score`: Data quality metrics

## Build and Push Docker Image

### Prerequisites

1. **Docker installed** and running
2. **Docker Hub account** (username: `hungvt0110`)
3. **Logged in to Docker Hub:**
   ```bash
   docker login -u hungvt0110
   ```

### Build and Push

```bash
cd spark-jobs/transform-crypto-silver
./build-image.sh
```

This will:

- Build image as `hungvt0110/transform-crypto-silver:latest`
- Push to Docker Hub automatically

## Configuration

### Environment Variables

| Variable                  | Default                                                                     | Description                                    |
| ------------------------- | --------------------------------------------------------------------------- | ---------------------------------------------- |
| `SPARK_MINOR_VERSION`     | `3.5`                                                                       | Spark version                                  |
| `ICEBERG_VERSION`         | `1.5.2`                                                                     | Iceberg version                                |
| `BRONZE_CATALOG_URL`      | `http://openhouse-lakekeeper:8181/catalog`                                  | Bronze catalog URL                             |
| `BRONZE_CLIENT_ID`        | `spark`                                                                     | Bronze OAuth2 client ID                        |
| `BRONZE_CLIENT_SECRET`    | `7PwnKrR0cRhh9PKaQRPRx2KGQBgmOUxa`                                          | Bronze OAuth2 client secret                    |
| `BRONZE_WAREHOUSE`        | `bronze`                                                                    | Bronze warehouse name                          |
| `SILVER_CATALOG_URL`      | `http://openhouse-lakekeeper:8181/catalog`                                  | Silver catalog URL                             |
| `SILVER_CLIENT_ID`        | `spark`                                                                     | Silver OAuth2 client ID                        |
| `SILVER_CLIENT_SECRET`    | `7PwnKrR0cRhh9PKaQRPRx2KGQBgmOUxa`                                          | Silver OAuth2 client secret                    |
| `SILVER_WAREHOUSE`        | `silver`                                                                    | Silver warehouse name                          |
| `KEYCLOAK_TOKEN_ENDPOINT` | `http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token` | Keycloak endpoint                              |
| `FILTER_INVALID_RECORDS`  | `false`                                                                     | Whether to filter out invalid records          |
| `DROP_EXISTING_TABLE`     | `false`                                                                     | Whether to drop existing table before creating |

### Command Line Arguments

```bash
spark-submit transform_crypto_silver.py [bronze_table] [silver_table] [filter_date]
```

- `bronze_table` (optional): Source Bronze table (default: `bronze.crypto_trades_raw`)
- `silver_table` (optional): Target Silver table (default: `silver.crypto_trades`)
- `filter_date` (optional): Filter Bronze data by date (format: `YYYY-MM-DD`)

## Local Development

```bash
# Set environment variables
export SPARK_MINOR_VERSION=3.5
export ICEBERG_VERSION=1.5.2
export BRONZE_CATALOG_URL=http://openhouse-lakekeeper:8181/catalog
export BRONZE_CLIENT_ID=spark
export BRONZE_CLIENT_SECRET=your-secret
export BRONZE_WAREHOUSE=bronze
export SILVER_CATALOG_URL=http://openhouse-lakekeeper:8181/catalog
export SILVER_CLIENT_ID=spark
export SILVER_CLIENT_SECRET=your-secret
export SILVER_WAREHOUSE=silver

# Run with spark-submit
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2 \
  transform_crypto_silver.py
```

## Data Quality

### Validation Rules

1. **Price**: > 0, within reasonable bounds
2. **Volume**: > 0, within reasonable bounds
3. **Timestamp**: Valid, not too old/future
4. **Symbol**: Not empty, valid format
5. **Currency**: Not empty, base != quote

### Quality Score

Calculated as: `1.0 - (error_count * 0.2)`, minimum 0.0

Each validation rule is worth 0.2 points.

## Transformations

1. **Data Cleaning**:

   - Uppercase exchange and currencies
   - Round price/volume to 8 decimal places
   - Normalize timestamps

2. **Data Enrichment**:

   - Generate unique `trade_id`
   - Calculate `trade_value = price × volume`
   - Extract `hour_of_day`, `day_of_week`
   - Parse trade conditions to flags

3. **Data Quality**:
   - Apply validation rules
   - Calculate quality score
   - Track validation errors

## Partitioning

- **Primary**: `trade_date` (daily)
- **Secondary**: `exchange`

## Troubleshooting

### Catalog Connection Issues

- Verify both catalogs are configured correctly
- Check OAuth2 credentials for both warehouses
- Ensure lakekeeper is accessible

### Validation Errors

- Check Bronze data quality
- Review validation rules in code
- Adjust bounds if needed

### Performance

- Use date filtering for incremental loads
- Consider partitioning strategy
- Monitor Spark executor resources

## Next Steps

After loading to Silver:

1. **Gold Layer**: Aggregate metrics and create analytical tables
2. **Monitoring**: Set up alerts for data quality scores
3. **Scheduling**: Use Airflow to run job periodically
