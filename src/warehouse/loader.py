"""Load Silver Parquet datasets into the PostgreSQL star schema."""

import calendar
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pyarrow.dataset as ds

from src.common.exceptions import PipelineError
from src.common.logger import get_logger
from src.config import Settings


logger = get_logger(__name__)

STAGING_COLUMNS = (
    "record_id",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance_miles",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type_id",
    "fare_amount",
    "extra_amount",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "pickup_date",
    "pickup_hour",
    "trip_duration_minutes",
    "average_speed_mph",
)


@dataclass(frozen=True)
class WarehouseLoadResult:
    """Auditable row counts from one warehouse load."""

    source_rows: int
    staging_rows: int
    inserted_rows: int
    existing_rows: int


class WarehouseLoader:
    """Load dimensions first, then facts through a temporary staging table."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def load(self, year: int, month: int) -> WarehouseLoadResult:
        enriched_path = (
            self.settings.project_root
            / "data"
            / "silver"
            / "enriched_trips"
            / f"year={year}"
            / f"month={month:02d}"
        )
        zones_path = self.settings.project_root / "data" / "silver" / "taxi_zones"
        weather_path = (
            self.settings.project_root
            / "data"
            / "silver"
            / "weather"
            / f"year={year}"
            / f"month={month:02d}"
        )
        for path in (enriched_path, zones_path, weather_path):
            if not path.exists():
                raise PipelineError(f"Required Silver dataset is missing: {path}")

        self._load_dimensions(year, month, zones_path, weather_path)
        source_dataset = ds.dataset(
            enriched_path, format="parquet", exclude_invalid_files=True
        )
        source_rows = source_dataset.count_rows()
        existing_period_rows = self._count_period_facts(year, month)
        if existing_period_rows == source_rows:
            logger.info(
                "Warehouse period already contains all %s source rows; skipping COPY.",
                source_rows,
            )
            return WarehouseLoadResult(
                source_rows=source_rows,
                staging_rows=0,
                inserted_rows=0,
                existing_rows=source_rows,
            )
        if existing_period_rows > source_rows:
            raise PipelineError(
                "Warehouse contains more period rows than the Silver source: "
                f"warehouse={existing_period_rows}, source={source_rows}."
            )
        staging_rows = self._copy_to_staging(source_dataset)
        if staging_rows != source_rows:
            raise PipelineError(
                f"Staging row mismatch: source={source_rows}, staging={staging_rows}."
            )
        inserted_rows, final_period_rows = self._insert_facts(year, month)
        if final_period_rows != source_rows:
            raise PipelineError(
                "Warehouse period count does not match Silver source: "
                f"source={source_rows}, warehouse={final_period_rows}."
            )
        self._truncate_staging()
        return WarehouseLoadResult(
            source_rows=source_rows,
            staging_rows=staging_rows,
            inserted_rows=inserted_rows,
            existing_rows=source_rows - inserted_rows,
        )

    def _load_dimensions(
        self, year: int, month: int, zones_path: Path, weather_path: Path
    ) -> None:
        zones = ds.dataset(
            zones_path, format="parquet", exclude_invalid_files=True
        ).to_table(
            columns=["location_id", "borough", "zone_name", "service_zone"]
        )
        weather = ds.dataset(
            weather_path, format="parquet", exclude_invalid_files=True
        ).to_table(
            columns=[
                "weather_hour",
                "temperature_celsius",
                "relative_humidity_percent",
                "precipitation_mm",
                "weather_code",
                "wind_speed_kmh",
            ]
        )
        final_day = calendar.monthrange(year, month)[1]
        date_start = f"{year}-{month:02d}-01"
        date_end = f"{year}-{month:02d}-{final_day:02d}"
        payment_types = (
            (0, "Unknown"),
            (1, "Credit card"),
            (2, "Cash"),
            (3, "No charge"),
            (4, "Dispute"),
            (5, "Unknown"),
            (6, "Voided trip"),
        )

        try:
            with psycopg.connect(self.settings.postgres_dsn) as connection:
                cursor = connection.cursor()
                cursor.executemany(
                    """
                    INSERT INTO warehouse.dim_location (
                        location_id, borough, zone_name, service_zone
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (location_id) DO UPDATE SET
                        borough = EXCLUDED.borough,
                        zone_name = EXCLUDED.zone_name,
                        service_zone = EXCLUDED.service_zone,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    [tuple(row.values()) for row in zones.to_pylist()],
                )
                cursor.executemany(
                    """
                    INSERT INTO warehouse.dim_weather (
                        weather_hour, temperature_celsius,
                        relative_humidity_percent, precipitation_mm,
                        weather_code, wind_speed_kmh
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (weather_hour) DO UPDATE SET
                        temperature_celsius = EXCLUDED.temperature_celsius,
                        relative_humidity_percent = EXCLUDED.relative_humidity_percent,
                        precipitation_mm = EXCLUDED.precipitation_mm,
                        weather_code = EXCLUDED.weather_code,
                        wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    [tuple(row.values()) for row in weather.to_pylist()],
                )
                cursor.executemany(
                    """
                    INSERT INTO warehouse.dim_payment_type (
                        payment_type_id, payment_type_name
                    ) VALUES (%s, %s)
                    ON CONFLICT (payment_type_id) DO UPDATE SET
                        payment_type_name = EXCLUDED.payment_type_name
                    """,
                    payment_types,
                )
                connection.execute(
                    """
                    INSERT INTO warehouse.dim_date (
                        date_key, full_date, year, quarter, month, month_name,
                        day_of_month, day_of_week, day_name, is_weekend
                    )
                    SELECT
                        TO_CHAR(day, 'YYYYMMDD')::INTEGER,
                        day::DATE,
                        EXTRACT(YEAR FROM day)::SMALLINT,
                        EXTRACT(QUARTER FROM day)::SMALLINT,
                        EXTRACT(MONTH FROM day)::SMALLINT,
                        TO_CHAR(day, 'FMMonth'),
                        EXTRACT(DAY FROM day)::SMALLINT,
                        EXTRACT(ISODOW FROM day)::SMALLINT,
                        TO_CHAR(day, 'FMDay'),
                        EXTRACT(ISODOW FROM day) IN (6, 7)
                    FROM generate_series(%s::DATE, %s::DATE, '1 day') AS day
                    ON CONFLICT (date_key) DO NOTHING
                    """,
                    (date_start, date_end),
                )
        except psycopg.Error as exc:
            raise PipelineError(f"Could not load warehouse dimensions: {exc}") from exc
        logger.info(
            "Loaded dimensions: locations=%s weather_hours=%s dates=%s payments=%s",
            zones.num_rows,
            weather.num_rows,
            final_day,
            len(payment_types),
        )

    def _copy_to_staging(self, source_dataset: ds.Dataset) -> int:
        copy_statement = (
            "COPY staging.enriched_trips ("
            + ", ".join(STAGING_COLUMNS)
            + ") FROM STDIN"
        )
        copied_rows = 0
        scanner = source_dataset.scanner(columns=list(STAGING_COLUMNS), batch_size=50_000)
        try:
            with psycopg.connect(self.settings.postgres_dsn) as connection:
                connection.execute("TRUNCATE staging.enriched_trips")
                connection.execute("SET LOCAL synchronous_commit = OFF")
                with connection.cursor().copy(copy_statement) as copy:
                    for batch in scanner.to_batches():
                        columns = [column.to_pylist() for column in batch.columns]
                        for row in zip(*columns):
                            copy.write_row(row)
                        copied_rows += batch.num_rows
                        if copied_rows % 500_000 < batch.num_rows:
                            logger.info("Copied %s rows into staging", copied_rows)
                connection.execute("ANALYZE staging.enriched_trips")
        except (psycopg.Error, OSError, ValueError) as exc:
            raise PipelineError(f"Could not COPY Silver data to staging: {exc}") from exc
        return copied_rows

    def _insert_facts(self, year: int, month: int) -> tuple[int, int]:
        insert_statement = """
            INSERT INTO warehouse.fact_trips (
                record_id, vendor_id, pickup_datetime, dropoff_datetime,
                pickup_date_key, pickup_location_key, dropoff_location_key,
                weather_key, payment_type_key, passenger_count,
                trip_distance_miles, trip_duration_minutes, average_speed_mph,
                fare_amount, extra_amount, mta_tax, tip_amount, tolls_amount,
                improvement_surcharge, congestion_surcharge, airport_fee,
                total_amount
            )
            SELECT
                s.record_id, s.vendor_id, s.pickup_datetime, s.dropoff_datetime,
                d.date_key, pickup.location_key, dropoff.location_key,
                w.weather_key, payment.payment_type_key, s.passenger_count,
                s.trip_distance_miles, s.trip_duration_minutes,
                s.average_speed_mph, s.fare_amount, s.extra_amount, s.mta_tax,
                s.tip_amount, s.tolls_amount, s.improvement_surcharge,
                s.congestion_surcharge, s.airport_fee, s.total_amount
            FROM staging.enriched_trips s
            JOIN warehouse.dim_date d ON d.full_date = s.pickup_date
            JOIN warehouse.dim_location pickup
                ON pickup.location_id = s.pickup_location_id
            JOIN warehouse.dim_location dropoff
                ON dropoff.location_id = s.dropoff_location_id
            JOIN warehouse.dim_weather w ON w.weather_hour = s.pickup_hour
            LEFT JOIN warehouse.dim_payment_type payment
                ON payment.payment_type_id = s.payment_type_id
            ON CONFLICT (record_id) DO NOTHING
        """
        try:
            with psycopg.connect(self.settings.postgres_dsn) as connection:
                matched_rows = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM staging.enriched_trips s
                    JOIN warehouse.dim_date d ON d.full_date = s.pickup_date
                    JOIN warehouse.dim_location pickup
                        ON pickup.location_id = s.pickup_location_id
                    JOIN warehouse.dim_location dropoff
                        ON dropoff.location_id = s.dropoff_location_id
                    JOIN warehouse.dim_weather w ON w.weather_hour = s.pickup_hour
                    """
                ).fetchone()[0]
                staging_rows = connection.execute(
                    "SELECT COUNT(*) FROM staging.enriched_trips"
                ).fetchone()[0]
                if matched_rows != staging_rows:
                    raise PipelineError(
                        f"Dimension join mismatch: staging={staging_rows}, matched={matched_rows}."
                    )
                cursor = connection.execute(insert_statement)
                inserted_rows = cursor.rowcount
                connection.execute("ANALYZE warehouse.fact_trips")
                final_period_rows = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM warehouse.fact_trips f
                    JOIN warehouse.dim_date d ON d.date_key = f.pickup_date_key
                    WHERE d.year = %s AND d.month = %s
                    """,
                    (year, month),
                ).fetchone()[0]
        except psycopg.Error as exc:
            raise PipelineError(f"Could not insert warehouse facts: {exc}") from exc
        return inserted_rows, final_period_rows

    def _truncate_staging(self) -> None:
        try:
            with psycopg.connect(self.settings.postgres_dsn) as connection:
                connection.execute("TRUNCATE staging.enriched_trips")
        except psycopg.Error as exc:
            raise PipelineError(f"Could not clear warehouse staging: {exc}") from exc

    def _count_period_facts(self, year: int, month: int) -> int:
        try:
            with psycopg.connect(self.settings.postgres_dsn) as connection:
                return connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM warehouse.fact_trips f
                    JOIN warehouse.dim_date d ON d.date_key = f.pickup_date_key
                    WHERE d.year = %s AND d.month = %s
                    """,
                    (year, month),
                ).fetchone()[0]
        except psycopg.Error as exc:
            raise PipelineError(f"Could not count existing warehouse facts: {exc}") from exc
