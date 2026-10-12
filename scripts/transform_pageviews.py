
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

INPUT_PATTERN = str(RAW_DIR / "*.gz")


def create_spark_session():
    return (
        SparkSession.builder
        .appName("WikipediaPageviewsTransformation")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    if not RAW_DIR.exists() or not list(RAW_DIR.glob("*.gz")):
        raise FileNotFoundError(
            f"No compressed raw pageview files found in {RAW_DIR}. "
            "Run scripts/ingest_pageviews.py first."
        )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Wikimedia pageview records have four whitespace-separated fields.
        raw_df = spark.read.text(INPUT_PATTERN)

        fields_df = raw_df.select(
            F.split(F.trim(F.col("value")), r"\s+").alias("fields")
        )

        parsed_df = fields_df.select(
            F.element_at("fields", 1).alias("domain_code"),
            F.element_at("fields", 2).alias("page_title"),
            F.element_at("fields", 3).alias("view_count"),
            F.element_at("fields", 4).alias("response_size"),
            F.size("fields").alias("field_count"),
        )

        # Convert numeric columns safely; malformed values become null.
        typed_df = parsed_df.select(
            F.col("domain_code"),
            F.regexp_replace(
                F.col("page_title"), "_", " "
            ).alias("page_title"),
            F.expr("try_cast(view_count AS BIGINT)").alias("view_count"),
            F.expr("try_cast(response_size AS BIGINT)").alias("response_size"),
            F.col("field_count"),
        )

        # Remove malformed records, empty fields, and negative values.
        clean_df = (
            typed_df
            .filter(F.col("field_count") == 4)
            .filter(F.col("domain_code").isNotNull())
            .filter(F.length(F.trim(F.col("domain_code"))) > 0)
            .filter(F.col("page_title").isNotNull())
            .filter(F.length(F.trim(F.col("page_title"))) > 0)
            .filter(F.col("view_count").isNotNull())
            .filter(F.col("response_size").isNotNull())
            .filter(F.col("view_count") >= 0)
            .filter(F.col("response_size") >= 0)
            .select(
                F.trim(F.col("domain_code")).cast(StringType()).alias("domain_code"),
                F.trim(F.col("page_title")).cast(StringType()).alias("page_title"),
                F.col("view_count").cast(LongType()).alias("view_count"),
                F.col("response_size").cast(LongType()).alias("response_size"),
            )
            .dropDuplicates(
                ["domain_code", "page_title", "view_count", "response_size"]
            )
            .withColumn("ingestion_date", F.current_date())
        )

        if not clean_df.take(1):
            raise ValueError(
                "No valid records remain after transformation. "
                "Check the input file format."
            )

        # Write a reusable, compressed Parquet dataset.
        clean_df.write.mode("overwrite").parquet(str(PROCESSED_DIR))

        print("Data transformation completed successfully.")
        print(f"Output directory: {PROCESSED_DIR}")
        print(f"Processed record count: {clean_df.count()}")
        clean_df.show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
