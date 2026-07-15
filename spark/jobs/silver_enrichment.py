"""Build Silver Taxi Zones, weather, and enriched Yellow Taxi trips."""

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


WEATHER_VARIABLES = (
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trips", required=True)
    parser.add_argument("--zones", required=True)
    parser.add_argument("--weather", required=True)
    parser.add_argument("--zones-output", required=True)
    parser.add_argument("--weather-output", required=True)
    parser.add_argument("--enriched-output", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    return parser.parse_args()


def clean_zones(spark: SparkSession, path: str) -> DataFrame:
    """Create a typed, unique Taxi Zone lookup."""
    source = spark.read.option("header", True).csv(path)
    zones = source.select(
        F.col("LocationID").cast("integer").alias("location_id"),
        F.trim("Borough").alias("borough"),
        F.trim("Zone").alias("zone_name"),
        F.trim("service_zone").alias("service_zone"),
    )
    if zones.count() != zones.select("location_id").distinct().count():
        raise RuntimeError("Taxi Zone LocationID is not unique.")
    return zones


def clean_weather(spark: SparkSession, path: str) -> DataFrame:
    """Explode parallel hourly arrays into one typed row per hour."""
    source = spark.read.option("multiLine", True).json(path)
    hourly = source.select(
        F.explode(
            F.arrays_zip(*[F.col(f"hourly.{name}") for name in WEATHER_VARIABLES])
        ).alias("hour")
    )
    weather = hourly.select(
        F.to_timestamp("hour.time").alias("weather_hour"),
        F.col("hour.temperature_2m").cast("double").alias("temperature_celsius"),
        F.col("hour.relative_humidity_2m")
        .cast("double")
        .alias("relative_humidity_percent"),
        F.col("hour.precipitation").cast("double").alias("precipitation_mm"),
        F.col("hour.weather_code").cast("integer").alias("weather_code"),
        F.col("hour.wind_speed_10m").cast("double").alias("wind_speed_kmh"),
    )
    if weather.count() != weather.select("weather_hour").distinct().count():
        raise RuntimeError("Weather hour is not unique.")
    return weather


def enrich_trips(trips: DataFrame, zones: DataFrame, weather: DataFrame) -> DataFrame:
    """Add pickup/drop-off geography and pickup-hour weather to every trip."""
    pickup_zones = zones.select(
        F.col("location_id").alias("pickup_zone_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone_name").alias("pickup_zone_name"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("dropoff_zone_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone_name").alias("dropoff_zone_name"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )

    return (
        trips.join(
            F.broadcast(pickup_zones),
            trips.pickup_location_id == pickup_zones.pickup_zone_location_id,
            "left",
        )
        .drop("pickup_zone_location_id")
        .join(
            F.broadcast(dropoff_zones),
            F.col("dropoff_location_id") == dropoff_zones.dropoff_zone_location_id,
            "left",
        )
        .drop("dropoff_zone_location_id")
        .join(
            F.broadcast(weather),
            F.col("pickup_hour") == weather.weather_hour,
            "left",
        )
        .drop("weather_hour")
        .withColumn("pickup_zone_matched", F.col("pickup_zone_name").isNotNull())
        .withColumn("dropoff_zone_matched", F.col("dropoff_zone_name").isNotNull())
        .withColumn("weather_matched", F.col("temperature_celsius").isNotNull())
    )


def write_deterministic(
    data: DataFrame,
    path: Path,
    partitions: int,
    key_column: str,
) -> None:
    prepared = (
        data.withColumn(
            "_output_partition",
            F.pmod(F.xxhash64(key_column), F.lit(partitions)),
        )
        .repartition(partitions, "_output_partition")
        .sortWithinPartitions(key_column)
        .drop("_output_partition")
    )
    prepared.write.mode("overwrite").option("compression", "snappy").parquet(
        str(path)
    )


def stabilize_part_names(directory: Path) -> None:
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


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("silver-taxi-enrichment")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    zones = clean_zones(spark, args.zones).cache()
    weather = clean_weather(spark, args.weather).cache()
    trips = spark.read.parquet(args.trips)
    enriched = enrich_trips(trips, zones, weather).cache()

    zone_rows = zones.count()
    weather_rows = weather.count()
    input_trip_rows = trips.count()
    enriched_trip_rows = enriched.count()
    missing_pickup_zones = enriched.filter(~F.col("pickup_zone_matched")).count()
    missing_dropoff_zones = enriched.filter(~F.col("dropoff_zone_matched")).count()
    missing_weather_hours = enriched.filter(~F.col("weather_matched")).count()

    if input_trip_rows != enriched_trip_rows:
        raise RuntimeError(
            f"Join changed row count: input={input_trip_rows}, output={enriched_trip_rows}."
        )

    zones_output = Path(args.zones_output)
    weather_output = Path(args.weather_output)
    enriched_output = Path(args.enriched_output)
    write_deterministic(zones, zones_output, 1, "location_id")
    write_deterministic(weather, weather_output, 1, "weather_hour")
    write_deterministic(enriched, enriched_output, 4, "record_id")

    enriched.unpersist()
    zones.unpersist()
    weather.unpersist()
    spark.stop()

    stabilize_part_names(zones_output)
    stabilize_part_names(weather_output)
    stabilize_part_names(enriched_output)
    metrics = {
        "year": args.year,
        "month": args.month,
        "zone_rows": zone_rows,
        "weather_rows": weather_rows,
        "input_trip_rows": input_trip_rows,
        "enriched_trip_rows": enriched_trip_rows,
        "missing_pickup_zones": missing_pickup_zones,
        "missing_dropoff_zones": missing_dropoff_zones,
        "missing_weather_hours": missing_weather_hours,
    }
    (enriched_output / "metrics.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(metrics, sort_keys=True))


if __name__ == "__main__":
    main()

