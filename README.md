# Wikipedia Pageviews Pipeline

An automated data engineering pipeline that collects Wikimedia pageview data, validates the input, transforms and aggregates the records using Apache Spark, stores analytical summaries in PostgreSQL, and orchestrates the workflow using Apache Airflow.

## 1. Project Overview

Wikimedia generates large volumes of pageview data. Processing these files manually makes it difficult to clean records, calculate page popularity, detect invalid data, and maintain reliable analytical reports.

This project implements an automated batch data pipeline to ingest Wikimedia pageview dumps, process them into structured Parquet datasets, generate daily summary metrics, and load the results into a PostgreSQL analytical warehouse.

### Objectives

- Automatically download Wikimedia pageview data.
- Validate the source file schema and record quality.
- Transform and clean raw data using PySpark.
- Generate aggregated pageview statistics.
- Store analytical summaries in PostgreSQL.
- Validate data quality and pipeline results.
- Schedule and monitor the workflow using Apache Airflow.
- Maintain source code and project documentation in GitHub.

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline scripts and validation |
| Apache Airflow | Workflow orchestration and scheduling |
| Apache Spark (PySpark) | Data transformation and aggregation |
| PostgreSQL | Analytical warehouse |
| Parquet | Processed and aggregated columnar data |
| Docker and Docker Compose | Containerized services |
| YAML | Schema and configuration |
| unittest | Automated tests |
| Git and GitHub | Version control and collaboration |

## 3. Architecture

```text
Wikimedia Pageview Dumps
          |
          v
   Data Ingestion
          |
          v
     Raw Data
       (GZIP)
          |
          v
   Schema Validation
          |
          v
 PySpark Transformation
          |
          v
 Processed Parquet Data
          |
          v
   Data Aggregation
          |
          +--------------------+
          |                    |
          v                    v
 Aggregated Parquet      PostgreSQL Warehouse
          |                    |
          +----------+---------+
                     |
                     v
             Data Quality Checks
                     |
                     v
            Apache Airflow DAG
       (orchestrates the workflow)
```

Apache Airflow coordinates the pipeline tasks and records their execution status. PySpark processes the data, while PostgreSQL stores the analytical summaries.

## 4. Project Structure

```text
WikipediaPageviewsPipeline/
│
├── dags/
│   └── wikipedia_pageviews_dag.py
│
├── config/
│   ├── airflow.cfg
│   └── schema.yaml
│
├── data/
│   ├── raw/
│   │   └── sample_pageviews.txt
│   ├── processed/
│   └── aggregated/
│
├── logs/
│
├── plugins/
│
├── scripts/
│   ├── check_schema.py
│   ├── ingest_pageviews.py
│   ├── transform_pageviews.py
│   ├── aggregate_pageviews.py
│   └── validate_pipeline.py
│
├── tests/
│   └── test_pipeline.py
│
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

Generated data files and runtime logs may be excluded from Git using `.gitignore`. The actual directory contents can differ depending on which files have been created locally.

## 5. Pipeline Implementation — Eight Steps

### Step 1: Project Setup and Infrastructure

Create the project directory and organize the folders for DAGs, configuration, data, scripts, tests, and logs.

Configure Docker Compose with the required Airflow services and PostgreSQL databases. Add the Dockerfile, dependency list, environment example, and Git ignore rules.

**Output:** A structured project with containerized services and configuration files.

### Step 2: Input Schema and Sample Data

Create `config/schema.yaml` to define the expected input fields:

- `domain_code`
- `page_title`
- `view_count`
- `response_size`

Create `data/raw/sample_pageviews.txt` containing representative Wikimedia records.

Implement `scripts/check_schema.py` to check field counts, numeric values, and nonnegative view counts and response sizes.

**Output:** A defined input format and a basic schema validation script.

### Step 3: Wikimedia Data Ingestion

Implement `scripts/ingest_pageviews.py` to retrieve Wikimedia pageview dump files.

The script attempts recent hourly dump URLs, downloads a valid available file, checks the GZIP content, and validates the records before storing the raw dump and metadata.

**Output:** Downloaded raw GZIP data and ingestion metadata under `data/raw/`.

**Note:** Wikimedia dumps can be large. Confirm the available disk space and avoid committing downloaded raw data to Git.

### Step 4: PySpark Transformation

Implement `scripts/transform_pageviews.py` to read the raw pageview data, parse the records, cast numeric columns, remove invalid records, normalize page titles, and remove duplicate records.

Write the cleaned output in Parquet format under `data/processed/`.

**Output:** Structured, cleaned pageview records suitable for aggregation.

### Step 5: Aggregation and PostgreSQL Loading

Implement `scripts/aggregate_pageviews.py` to aggregate processed data by domain and ingestion date.

The summary includes:

- `total_views`
- `total_response_size`
- `page_count`
- `average_views`

Write the aggregated results to `data/aggregated/` in Parquet format and upsert the summary rows into the PostgreSQL `pageview_daily_summary` table.

**Output:** Aggregated Parquet datasets and analytical records in PostgreSQL.

### Step 6: Data Quality Validation

Implement `scripts/validate_pipeline.py` to check processed data, aggregated results, and the PostgreSQL warehouse.

Validation includes required columns, null and negative values, duplicate records, unique aggregation keys, expected aggregate calculations, and consistency between the aggregated Parquet data and warehouse records.

**Output:** A validation result indicating whether the pipeline data meets the implemented checks.

### Step 7: Automated Testing

Implement `tests/test_pipeline.py` using Python `unittest`.

Tests cover:

- Required schema and sample files.
- Expected schema fields.
- Valid sample records.
- Records with missing or extra fields.
- Nonnumeric values.
- Negative values.

Run the tests with:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

**Output:** Automated test results for the schema and record-validation rules.

These unit tests do not replace a full integration test of Airflow, Spark, and PostgreSQL.

### Step 8: Airflow Integration and End-to-End Execution

Create `dags/wikipedia_pageviews_dag.py` to orchestrate the following tasks:

1. `ingest_pageviews`
2. `transform_pageviews`
3. `aggregate_pageviews`
4. `validate_pipeline`

Configure task dependencies so that each stage starts after its predecessor succeeds. Configure retries and disable historical catch-up runs.

**Output:** An Airflow-managed pipeline that can be triggered and monitored through the Airflow interface.

## 6. Prerequisites

Install or prepare:

- Docker Desktop for Windows.
- Git.
- Visual Studio Code.
- Python compatible with the project dependencies.
- Sufficient disk space for raw Wikimedia dumps and processed data.

Start Docker Desktop before running Docker Compose commands.

## 7. Installation and Setup

Open the VS Code terminal using PowerShell.

Navigate to the project folder:

```powershell
cd "$HOME\Downloads\WikipediaPageviewsPipeline"
```

Check Docker:

```powershell
docker --version
docker compose version
```

Check Git and Python:

```powershell
git --version
python --version
```

Start the Docker services:

```powershell
docker compose up -d --build
```

Inspect their status:

```powershell
docker compose ps
```

**Configuration requirements**

- Configure the Airflow and PostgreSQL credentials through the environment settings used by your Compose file.
- Keep real secrets in local environment configuration; do not commit them to GitHub.
- Verify that the Airflow container can access the project scripts, data directories, and warehouse service.
- Confirm that the Airflow image contains the dependencies needed by the pipeline scripts.

The commands and service names above assume they match your local `docker-compose.yaml`.

## 8. Running the Pipeline

### A. Check the sample schema

```powershell
python scripts/check_schema.py
```

### B. Run ingestion

Run ingestion in the container where the script and its dependencies are available:

```powershell
docker compose exec airflow-webserver python /opt/airflow/scripts/ingest_pageviews.py
```

### C. Run transformation

```powershell
docker compose exec airflow-webserver python /opt/airflow/scripts/transform_pageviews.py
```

### D. Run aggregation

```powershell
docker compose exec airflow-webserver python /opt/airflow/scripts/aggregate_pageviews.py
```

### E. Run data validation

```powershell
docker compose exec airflow-webserver python /opt/airflow/scripts/validate_pipeline.py
```

The commands above run individual stages manually. They require the necessary files, data, dependencies, environment variables, and running warehouse service to be available in the container.

### F. Run automated tests

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

Run this from the project root using the Python environment where the project is configured.

## 9. Running with Apache Airflow

Start the services:

```powershell
docker compose up -d
```

List available DAGs:

```powershell
docker compose exec airflow-webserver airflow dags list
```

Open the Airflow web interface:

http://localhost:8080

Then:

1. Sign in using the credentials configured in your Docker Compose environment.
2. Find `wikipedia_pageviews_pipeline`.
3. Enable the DAG.
4. Trigger a DAG run.
5. Open the Grid view to monitor the tasks.
6. Open task logs if a task fails.

The DAG requires the scripts to be mounted at `/opt/airflow/scripts` and the data directories to be mounted consistently between tasks. The services must be able to communicate over the Docker Compose network.

## 10. Verify the Results

Inspect the output directories from PowerShell:

```powershell
Get-ChildItem data\raw
Get-ChildItem data\processed
Get-ChildItem data\aggregated
```

Check the PostgreSQL summary table:

```powershell
docker compose exec warehouse-db psql -U pipeline_user -d pageviews -c "SELECT * FROM pageview_daily_summary LIMIT 10;"
```

This query assumes the database service, database name, and user match your Compose configuration.

A successful run should produce raw data, processed Parquet files, aggregated Parquet files, and summary rows in PostgreSQL. The actual output depends on the available source dumps and successful task execution.

## 11. Error Handling and Troubleshooting

### Docker services are not running

```powershell
docker compose ps
docker compose logs --tail 100
```

Start the services if necessary:

```powershell
docker compose up -d --build
```

### Airflow cannot find the DAG

```powershell
docker compose logs --tail 100 airflow-scheduler
```

Verify that the DAG file is mounted into the Airflow DAG directory and that the Python file has no syntax or import errors.

### Pipeline script is missing

Verify the script exists in your project and is mounted at the expected container path:

```powershell
Test-Path scripts\ingest_pageviews.py
docker compose exec airflow-webserver ls /opt/airflow/scripts
```

### PostgreSQL connection fails

Check the warehouse service:

```powershell
docker compose ps warehouse-db
docker compose logs --tail 100 warehouse-db
```

Verify the host, port, database, username, and password used by the aggregation and validation scripts.

### No processed or aggregated files

Check that ingestion succeeded and produced usable input. Then inspect the transformation and aggregation task logs. The scripts must use compatible input and output paths.

### Automated tests fail

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

Read the failing test output, check the sample records and schema, and rerun the tests after correcting the problem.

## 12. Git and GitHub

Check changed files:

```powershell
git status
```

Add and commit the project changes:

```powershell
git add .
git commit -m "Complete Wikipedia pageviews data pipeline"
git push origin main
```

Before committing, confirm that `.env` files containing secrets, virtual environments, large raw dumps, and generated data are excluded as intended by `.gitignore`.

If your repository uses a different default branch, use that branch instead of `main`.

## 13. Expected Deliverables

At completion, the repository should contain:

- A Docker-based Airflow and PostgreSQL setup.
- Wikimedia ingestion code.
- Schema configuration and sample data.
- PySpark transformation code.
- Aggregation and warehouse loading code.
- Data quality validation code.
- Automated tests.
- An Airflow DAG.
- Setup, execution, troubleshooting, and architecture documentation.

The deliverables are considered complete only after the scripts and infrastructure have been run and verified successfully.

## 14. Future Enhancements

- Add configurable date ranges for historical backfills.
- Add stronger schema evolution support.
- Implement structured logging and alert notifications.
- Add retry-aware ingestion and source download checksums.
- Add unit and integration tests for Spark and PostgreSQL.
- Track pipeline metrics and data quality trends.
- Optimize processing for large Wikimedia datasets.
- Add CI checks for tests and code quality.

## 15. Conclusion

The Wikipedia Pageviews Pipeline demonstrates an end-to-end batch data engineering workflow using Python, Apache Airflow, PySpark, Parquet, PostgreSQL, and Docker. It automates ingestion, transformation, aggregation, and data validation, while providing an orchestrated workflow for repeatable execution and monitoring.
