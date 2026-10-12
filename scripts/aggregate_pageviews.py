
import os
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
AGGREGATED_DIR = PROJECT_DIR / "data" / "aggregated"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("WikipediaPageviewsAggregation")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


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


def create_table(connection):
    sql = """
    CREATE TABLE IF NOT EXISTS pageview_daily_summary (
        domain_code VARCHAR(255) NOT NULL,
        ingestion_date DATE NOT NULL,
        total_views BIGINT NOT NULL,
        total_response_size BIGINT NOT NULL,
        page_count BIGINT NOT NULL,
        average_views DOUBLE PRECISION NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (domain_code, ingestion_date)
    );
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.commit()


def load_to_postgres(connection, rows):
    sql = """
    INSERT INTO pageview_daily_summary (
        domain_code,
        ingestion_date,
        total_views,
        total_response_size,
        page_count,
        average_views
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (domain_code, ingestion_date)
    DO UPDATE SET
        total_views = EXCLUDED.total_views,
        total_response_size = EXCLUDED.total_response_size,
        page_count = EXCLUDED.page_count,
        average_views = EXCLUDED.average_views,
        updated_at = CURRENT_TIMESTAMP;
    """

    with connection.cursor() as cursor:
        for row in rows:
            cursor.execute(sql, row)

    connection.commit()


def main():
    if not PROCESSED_DIR.exists():
        raise FileNotFoundError(
            f"Processed data directory not found: {PROCESSED_DIR}. "
            "Run Step 4 first."
        )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        source_df = spark.read.parquet(str(PROCESSED_DIR))

        # Aggregate the processed data by domain and ingestion date.
        summary_df = (
            source_df
            .groupBy("domain_code", "ingestion_date")
            .agg(
                F.sum("view_count").alias("total_views"),
                F.sum("response_size").alias("total_response_size"),
                F.count("*").alias("page_count"),
                F.avg("view_count").alias("average_views"),
            )
            .withColumn(
                "average_views",
                F.round(F.col("average_views"), 2),
            )
        )

        if not summary_df.take(1):
            raise ValueError("No processed records are available to aggregate")

        AGGREGATED_DIR.mkdir(parents=True, exist_ok=True)

        summary_df.write.mode("overwrite").parquet(
            str(AGGREGATED_DIR)
        )

        # This aggregation is intentionally small: one row per
        # domain and ingestion date.
        rows = [
            (
                row["domain_code"],
                row["ingestion_date"],
                row["total_views"],
                row["total_response_size"],
                row["page_count"],
                float(row["average_views"]),
            )
            for row in summary_df.collect()
        ]

        with get_database_connection() as connection:
            create_table(connection)
            load_to_postgres(connection, rows)

        print("Aggregation and PostgreSQL loading completed successfully.")
        print(f"Aggregated Parquet output: {AGGREGATED_DIR}")
        print(f"Summary rows loaded into PostgreSQL: {len(rows)}")
        summary_df.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
