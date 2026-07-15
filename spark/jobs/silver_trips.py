"""Clean one monthly Yellow Taxi Parquet file into Silver datasets."""

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    return parser.parse_args()


def standardize(source: DataFrame) -> DataFrame:
    """Select stable names and types, then add reusable derived columns."""
    selected = source.select(
        F.col("VendorID").cast("integer").alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        F.col("passenger_count").cast("integer").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance_miles"),
        F.col("RatecodeID").cast("integer").alias("rate_code_id"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_forward_flag"),
        F.col("PULocationID").cast("integer").alias("pickup_location_id"),
        F.col("DOLocationID").cast("integer").alias("dropoff_location_id"),
        F.col("payment_type").cast("integer").alias("payment_type_id"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra_amount"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias(
            "improvement_surcharge"
        ),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias(
            "congestion_surcharge"
        ),
        F.col("Airport_fee").cast("double").alias("airport_fee"),
    )
    selected = selected.withColumn(
        "record_id",
        F.sha2(F.to_json(F.struct(*[F.col(name) for name in selected.columns])), 256),
    )
    return (
        selected.withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.date_trunc("hour", "pickup_datetime"))
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("dropoff_datetime")
                - F.unix_timestamp("pickup_datetime")
            )
            / F.lit(60.0),
        )
        .withColumn(
            "average_speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.col("trip_distance_miles")
                / (F.col("trip_duration_minutes") / F.lit(60.0)),
            ),
        )
    )


def classify(data: DataFrame, year: int, month: int) -> DataFrame:
    """Attach explicit rejection reasons while keeping every input row."""
    duplicate_window = Window.partitionBy("record_id").orderBy(F.lit(1))
    ranked = data.withColumn("duplicate_rank", F.row_number().over(duplicate_window))
    return ranked.withColumn(
        "rejection_reason",
        F.concat_ws(
            "|",
            F.when(F.col("pickup_datetime").isNull(), "missing_pickup_datetime"),
            F.when(F.col("dropoff_datetime").isNull(), "missing_dropoff_datetime"),
            F.when(
                F.col("dropoff_datetime") <= F.col("pickup_datetime"),
                "dropoff_not_after_pickup",
            ),
            F.when(
                (F.year("pickup_datetime") != year)
                | (F.month("pickup_datetime") != month),
                "pickup_outside_requested_month",
            ),
            F.when(
                (F.col("trip_duration_minutes") <= 0)
                | (F.col("trip_duration_minutes") > 360),
                "invalid_trip_duration",
            ),
            F.when(
                (F.col("trip_distance_miles") <= 0)
                | (F.col("trip_distance_miles") > 100),
                "invalid_trip_distance",
            ),
            F.when(F.col("fare_amount") < 0, "negative_fare_amount"),
            F.when(F.col("total_amount") < 0, "negative_total_amount"),
            F.when(
                (F.col("pickup_location_id").isNull())
                | (F.col("pickup_location_id") <= 0),
                "invalid_pickup_location",
            ),
            F.when(
                (F.col("dropoff_location_id").isNull())
                | (F.col("dropoff_location_id") <= 0),
                "invalid_dropoff_location",
            ),
            F.when(
                F.col("passenger_count").isNotNull()
                & ~F.col("passenger_count").between(1, 8),
                "invalid_passenger_count",
            ),
            F.when(F.col("duplicate_rank") > 1, "duplicate_record"),
        ),
    )


def stabilize_part_names(directory: Path) -> None:
    """Remove Spark marker files and give Parquet parts deterministic names."""
    part_files = sorted(directory.glob("part-*.parquet"))
    temporary_files: list[Path] = []
    for index, source in enumerate(part_files):
        temporary = directory / f"temporary-{index:05d}.parquet"
        source.replace(temporary)
        temporary_files.append(temporary)
    for index, temporary in enumerate(temporary_files):
        temporary.replace(directory / f"part-{index:05d}.parquet")
    for marker in directory.iterdir():
        if marker.is_file() and marker.name.startswith(("_", ".")):
            marker.unlink()


def write_deterministic(data: DataFrame, path: Path, partitions: int) -> None:
    """Write stable files by hashing and sorting on the full-row record ID."""
    prepared = (
        data.withColumn(
            "_output_partition",
            F.pmod(F.xxhash64("record_id"), F.lit(partitions)),
        )
        .repartition(partitions, "_output_partition")
        .sortWithinPartitions("record_id")
        .drop("_output_partition")
    )
    prepared.write.mode("overwrite").option("compression", "snappy").parquet(
        str(path)
    )


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("silver-yellow-taxi-trips")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    output_root = Path(args.output)
    accepted_path = output_root / "accepted"
    rejected_path = output_root / "rejected"

    source = spark.read.parquet(args.input)
    classified = classify(standardize(source), args.year, args.month).cache()
    accepted = classified.filter(F.col("rejection_reason") == "").drop(
        "duplicate_rank", "rejection_reason"
    )
    rejected = classified.filter(F.col("rejection_reason") != "").drop(
        "duplicate_rank"
    )

    input_rows = classified.count()
    accepted_rows = accepted.count()
    rejected_rows = rejected.count()
    if input_rows != accepted_rows + rejected_rows:
        raise RuntimeError("Silver row accounting failed.")

    write_deterministic(accepted, accepted_path, partitions=4)
    write_deterministic(rejected, rejected_path, partitions=2)
    classified.unpersist()
    spark.stop()

    stabilize_part_names(accepted_path)
    stabilize_part_names(rejected_path)
    metrics = {
        "year": args.year,
        "month": args.month,
        "input_rows": input_rows,
        "accepted_rows": accepted_rows,
        "rejected_rows": rejected_rows,
    }
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "metrics.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(metrics, sort_keys=True))


if __name__ == "__main__":
    main()
