
# Wikipedia Pageviews Data Pipeline

## Overview

A data engineering project designed to automate hourly pageview
data processing using Apache Airflow, PySpark, and PostgreSQL.

## Technology Stack

- Python
- Apache Airflow
- PySpark
- PostgreSQL
- Docker and Docker Compose
- Pandas

## Project Structure

- `dags/` - Airflow DAGs
- `data/raw/` - Raw source data
- `data/processed/` - Processed data
- `data/aggregated/` - Aggregated outputs
- `logs/` - Airflow logs
- `plugins/` - Airflow plugins
- `scripts/` - Supporting scripts
- `tests/` - Automated tests

## Local Development

1. Install Docker Desktop.
2. Start Docker Desktop.
3. Open PowerShell in the project directory.
4. Validate the Compose configuration:

   `docker compose config`

5. Build and start the environment:

   `docker compose up --build -d`

6. Open http://localhost:8080.

## Local Airflow Login

- Username: `admin`
- Password: `admin`

These credentials are for local development only.
