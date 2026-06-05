# Scalable Data Quality Validation Pipeline with Apache Spark & Docker

[![GitHub](https://img.shields.io/badge/GitHub-886ss%2Fspark--dq--pipeline-blue?logo=github)](https://github.com/886ss/spark-dq-pipeline)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker)](https://www.docker.com/)
[![Spark](https://img.shields.io/badge/Spark-3.4.0-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](./LICENSE)

## Project Goal

A robust, scalable, configurable framework for executing data quality (DQ) checks on large datasets within ETL processes. Uses **Apache Spark** for distributed processing and **Docker** for consistent, portable deployment.

## Technology Stack

- **Core Processing:** Apache Spark 3.4.1
- **Language:** Python 3.9
- **Key Libraries:** PySpark, Pandas, NumPy, PyYAML, psycopg2, SciPy
- **Containerization:** Docker, Docker Compose
- **Database:** PostgreSQL 15

## Project Setup & Execution

### Prerequisites
- Docker Desktop (>= 4GB RAM recommended)

### Steps

```powershell
# 1. Build the Docker image
docker compose build

# 2. Start PostgreSQL
docker compose up -d postgres-db

# 3. Create database table (one-time)
docker compose exec postgres-db psql -U dq_user -d dq_db -c "
CREATE TABLE IF NOT EXISTS dq_results_log (
  log_id SERIAL PRIMARY KEY,
  check_run_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  check_name VARCHAR(255) NOT NULL,
  dq_check_timestamp TIMESTAMPTZ,
  status VARCHAR(10) NOT NULL CHECK (status IN ('PASS', 'FAIL', 'ERROR')),
  metric DOUBLE PRECISION,
  message TEXT,
  parameters JSONB,
  details JSONB,
  data_source VARCHAR(1024),
  pipeline_run_id VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_dq_log_check_name ON dq_results_log (check_name);
CREATE INDEX IF NOT EXISTS idx_dq_log_run_ts ON dq_results_log (check_run_timestamp);
CREATE INDEX IF NOT EXISTS idx_dq_log_status ON dq_results_log (status);
"

# 4. Generate synthetic data
docker compose run --rm spark-dq-app python src/data_generator.py
docker compose run --rm spark-dq-app python src/generate_users_data.py

# 5. Run the DQ pipeline
docker compose run --rm spark-dq-app python src/main_pipeline.py
```

## Core Pipeline Architecture

1. Load Configuration (YAML)
2. Initialize Spark Session
3. Load Raw Data (Parquet)
4. Apply Cleaning & Deduplication
5. Execute DQ Checks (completeness, uniqueness, validity, consistency, format, stats, referential)
6. Generate Reports (JSON/CSV)
7. Log to PostgreSQL

## DQ Check Categories

- **Completeness:** Null value detection
- **Uniqueness:** Duplicate detection
- **Validity:** Range, regex, categorical checks
- **Consistency:** Custom SQL conditions, referential integrity
- **Format:** String length, date format, data type verification
- **Statistical:** Mean deviation from expected values
