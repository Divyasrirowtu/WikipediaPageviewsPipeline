
import os
import sys
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
AGGREGATED_DIR = PROJECT_DIR / "data" / "aggregated"

REQUIRED_PROCESSED_COLUMNS = {
    "domain_code",
    "page_title",
    "view_count",
    "response_size",
    "ingestion_date",
}

REQUIRED_AGGREGATED_COLUMNS = {
    "domain_code",
    "ingestion_date",
    "total_views",
    "total_response_size",
    "page_count",
    "average_views",
}


def check(condition, message):
    """Report a validation result and fail if the condition is false."""
    if not condition:
        raise AssertionError(message)

    print(f"PASS: {message}")


def get_database_connection():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "warehouse-db"),
        port=int(os.getenv("WAREHOUSE_PORT", "5432")),
        dbname=os.getenv("WAREHOUSE_DB", "pageviews"),
        user=os.getenv("WAREHOUSE_USER", "pipeline_user"),
        password=os.getenv(
            "WAREHOUSE_PASSWORD", "warehouse_dev_password"
        ),
        connect_timeout=10,
    )


def main():
    if not PROCESSED_DIR.exists():
        raise FileNotFoundError(
            f"Processed data not found: {PROCESSED_DIR}. Run Step 4 first."
        )

    if not AGGREGATED_DIR.exists():
        raise FileNotFoundError(
            f"Aggregated data not found: {AGGREGATED_DIR}. Run Step 5 first."
        )

    spark = (
        SparkSession.builder
        .appName("WikipediaPageviewsQualityValidation")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Validate the processed Parquet dataset.
        processed = spark.read.parquet(str(PROCESSED_DIR))
        processed_columns = set(processed.columns)

        check(
            REQUIRED_PROCESSED_COLUMNS.issubset(processed_columns),
            "Processed dataset contains all required columns",
        )

        processed_count = processed.count()
        check(processed_count > 0, "Processed dataset is not empty")

        invalid_processed = processed.filter(
            F.col("domain_code").isNull()
            | (F.length(F.trim(F.col("domain_code"))) == 0)
            | F.col("page_title").isNull()
            | (F.length(F.trim(F.col("page_title"))) == 0)
            | F.col("view_count").isNull()
            | F.col("response_size").isNull()
            | F.col("ingestion_date").isNull()
            | (F.col("view_count") < 0)
            | (F.col("response_size") < 0)
        ).count()

        check(
            invalid_processed == 0,
            "Processed data has no null or invalid required values",
        )

        duplicate_processed = (
            processed.groupBy(
                "domain_code",
                "page_title",
                "view_count",
                "response_size",
            )
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        check(
            duplicate_processed == 0,
            "Processed data has no exact duplicate records",
        )

        # 2. Validate the aggregated Parquet dataset.
        aggregated = spark.read.parquet(str(AGGREGATED_DIR))
        aggregated_columns = set(aggregated.columns)

        check(
            REQUIRED_AGGREGATED_COLUMNS.issubset(aggregated_columns),
            "Aggregated dataset contains all required columns",
        )

        aggregated_count = aggregated.count()
        check(
            aggregated_count > 0,
            "Aggregated dataset is not empty",
        )

        invalid_aggregated = aggregated.filter(
            F.col("domain_code").isNull()
            | F.col("ingestion_date").isNull()
            | F.col("total_views").isNull()
            | F.col("total_response_size").isNull()
            | F.col("page_count").isNull()
            | F.col("average_views").isNull()
            | (F.col("total_views") < 0)
            | (F.col("total_response_size") < 0)
            | (F.col("page_count") <= 0)
            | (F.col("average_views") < 0)
        ).count()

        check(
            invalid_aggregated == 0,
            "Aggregated data has no invalid required values",
        )

        duplicate_aggregates = (
            aggregated.groupBy("domain_code", "ingestion_date")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        check(
            duplicate_aggregates == 0,
            "Aggregated data has unique domain/date keys",
        )

        # 3. Compare aggregates with the processed data.
        expected = processed.groupBy(
            "domain_code", "ingestion_date"
        ).agg(
            F.sum("view_count").alias("expected_views"),
            F.sum("response_size").alias("expected_response_size"),
            F.count("*").alias("expected_page_count"),
            F.avg("view_count").alias("expected_average"),
        )

        comparison = expected.alias("e").join(
            aggregated.alias("a"),
            on=["domain_code", "ingestion_date"],
            how="full_outer",
        )

        mismatches = comparison.filter(
            F.col("e.expected_views").isNull()
            | F.col("a.total_views").isNull()
            | (F.col("e.expected_views") != F.col("a.total_views"))
            | (
                F.col("e.expected_response_size")
                != F.col("a.total_response_size")
            )
            | (F.col("e.expected_page_count") != F.col("a.page_count"))
            | (
                F.abs(
                    F.col("e.expected_average")
                    - F.col("a.average_views")
                ) > 0.011
            )
        ).count()

        check(
            mismatches == 0,
            "Aggregated totals match the processed dataset",
        )

        # 4. Validate the PostgreSQL warehouse.
        with get_database_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        domain_code,
                        ingestion_date,
                        total_views,
                        total_response_size,
                        page_count,
                        average_views
                    FROM pageview_daily_summary
                    """
                )
                database_rows = cursor.fetchall()

        check(
            len(database_rows) > 0,
            "PostgreSQL warehouse contains summary records",
        )

        database_summary = spark.createDataFrame(
            database_rows,
            schema=[
                "domain_code",
                "ingestion_date",
                "total_views",
                "total_response_size",
                "page_count",
                "average_views",
            ],
        )

        database_mismatches = aggregated.alias("a").join(
            database_summary.alias("d"),
            on=["domain_code", "ingestion_date"],
            how="full_outer",
        ).filter(
            F.col("a.total_views").isNull()
            | F.col("d.total_views").isNull()
            | (F.col("a.total_views") != F.col("d.total_views"))
            | (
                F.col("a.total_response_size")
                != F.col("d.total_response_size")
            )
            | (F.col("a.page_count") != F.col("d.page_count"))
            | (
                F.abs(
                    F.col("a.average_views") - F.col("d.average_views")
                ) > 0.011
            )
        ).count()

        check(
            database_mismatches == 0,
            "PostgreSQL summary matches the aggregated Parquet data",
        )

        print("\nAll data quality checks passed.")
        print(f"Processed records: {processed_count}")
        print(f"Aggregated records: {aggregated_count}")
        print(f"PostgreSQL summary records: {len(database_rows)}")

    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"\nVALIDATION FAILED: {error}", file=sys.stderr)
        sys.exit(1)
