
# =====================================================================
# UC-12 Grid Sensor Anomaly Watch - FILE 1 of 2: BRONZE -> SILVER
# Lakeflow pipeline: Python, serverless
#
# Default catalog = uc_grid_sensor
# Default schema  = grid_silver
#
# Required schemas:
#   uc_grid_sensor.grid_bronze
#   uc_grid_sensor.grid_silver
#   uc_grid_sensor.grid_gold
#
# Creates:
#   silver_asset_registry (AUTO CDC, SCD TYPE 2)
#   silver_grid_telemetry
#   silver_telemetry_rejects
# =====================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

CATALOG = "uc_grid_sensor"
BRONZE = f"{CATALOG}.grid_bronze"
SILVER = f"{CATALOG}.grid_silver"


# =====================================================================
# SILVER 1: ASSET REGISTRY -> AUTO CDC + SCD TYPE 2
# =====================================================================

@dp.temporary_view()
def registry_changes():
    return (
        spark.readStream
        .table(f"{BRONZE}.bronze_asset_registry")
        .drop("__START_AT", "__END_AT")
        .withColumn("asset_id", F.upper(F.trim("asset_id")))
        .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
    )


dp.create_streaming_table(
    name=f"{SILVER}.silver_asset_registry",
    comment="SCD Type 2 asset registry preserving historical asset versions."
)


dp.create_auto_cdc_flow(
    target=f"{SILVER}.silver_asset_registry",
    source="registry_changes",
    keys=["asset_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=2,
)


# =====================================================================
# SILVER 2: TELEMETRY VALIDATION AND CLEANING
# Any missing or invalid required field -> REJECT
# =====================================================================

@dp.temporary_view()
def telemetry_checked():

    raw = spark.read.table(
        f"{BRONZE}.bronze_grid_telemetry"
    )

    # Approved assets from the registry
    known_assets = (
        spark.read.table(f"{BRONZE}.bronze_asset_registry")
        .select(
            F.upper(F.trim("asset_id")).alias("known_id")
        )
        .distinct()
    )

    # Parse timestamps and sensor values
    df = (
        raw
        .withColumn(
            "asset_id",
            F.upper(F.trim("asset_id"))
        )
        .withColumn(
            "event_ts",
            F.expr(
                "try_to_timestamp(cast(event_timestamp as string))"
            )
        )
        .withColumn(
            "voltage_kv",
            F.expr("try_cast(voltage_kv as double)")
        )
        .withColumn(
            "frequency_hz",
            F.expr("try_cast(frequency_hz as double)")
        )
        .withColumn(
            "temperature_c",
            F.expr("try_cast(temperature_c as double)")
        )
        .join(
            F.broadcast(known_assets),
            F.col("asset_id") == F.col("known_id"),
            "left"
        )
    )

    # ---------------------------------------------------------------
    # Validate sensor values BEFORE assigning rejection reasons.
    # Missing, unparseable, or out-of-range values are invalid.
    # ---------------------------------------------------------------

    df = (
        df
        .withColumn(
            "voltage_invalid",
            F.col("voltage_kv").isNull()
            | (F.col("voltage_kv") <= 0)
            | (F.col("voltage_kv") >= 20)
        )
        .withColumn(
            "frequency_invalid",
            F.col("frequency_hz").isNull()
            | (~F.col("frequency_hz").between(40, 60))
        )
        .withColumn(
            "temperature_invalid",
            F.col("temperature_c").isNull()
            | (~F.col("temperature_c").between(-40, 200))
        )
    )

    # ---------------------------------------------------------------
    # Identify repeated event IDs.
    # Multiple readings from the same asset are allowed.
    # ---------------------------------------------------------------

    w = Window.partitionBy("event_id").orderBy(
        F.col("event_ts").asc_nulls_last()
    )

    df = df.withColumn(
        "dup_rank",
        F.row_number().over(w)
    )

    # ---------------------------------------------------------------
    # Assign one rejection reason per record.
    # First matching reason wins.
    # ---------------------------------------------------------------

    return df.withColumn(
        "reject_reason",

        F.when(
            F.col("event_id").isNull(),
            "MISSING_EVENT_ID"
        )
        .when(
            F.col("known_id").isNull(),
            "UNKNOWN_ASSET"
        )
        .when(
            F.col("event_ts").isNull()
            | (F.year("event_ts") < 2026),
            "BAD_TIMESTAMP"
        )
        .when(
            F.col("dup_rank") > 1,
            "DUPLICATE_EVENT"
        )
        .when(
            F.col("voltage_invalid"),
            "INVALID_VOLTAGE"
        )
        .when(
            F.col("frequency_invalid"),
            "INVALID_FREQUENCY"
        )
        .when(
            F.col("temperature_invalid"),
            "INVALID_TEMPERATURE"
        )
    )


# =====================================================================
# SILVER OUTPUT 1: ONLY FULLY VALID TELEMETRY RECORDS
# =====================================================================

@dp.materialized_view(
    name=f"{SILVER}.silver_grid_telemetry",
    comment="Only telemetry records with valid assets, timestamps, unique event IDs, and complete sensor readings."
)
def silver_grid_telemetry():

    return (
        spark.read.table("telemetry_checked")
        .filter("reject_reason IS NULL")

        # Derive asset type from the approved asset ID prefix
        .withColumn(
            "asset_type",
            F.when(
                F.col("asset_id").startswith("GN"),
                "Generator"
            )
            .when(
                F.col("asset_id").startswith("TR"),
                "Transformer"
            )
            .when(
                F.col("asset_id").startswith("CB"),
                "Circuit Breaker"
            )
        )

        .select(
            "event_id",
            "asset_id",
            "asset_type",
            "event_ts",
            "voltage_kv",
            "frequency_hz",
            "temperature_c",
            "source_system"
        )
    )


# =====================================================================
# SILVER OUTPUT 2: REJECTED TELEMETRY RECORDS
# =====================================================================

@dp.materialized_view(
    name=f"{SILVER}.silver_telemetry_rejects",
    comment="Telemetry records rejected for missing or invalid fields, unknown assets, bad timestamps, or duplicate event IDs."
)
def silver_telemetry_rejects():

    return (
        spark.read.table("telemetry_checked")
        .filter("reject_reason IS NOT NULL")
        .select(
            "event_id",
            "asset_id",
            "event_timestamp",
            "event_ts",
            "voltage_kv",
            "frequency_hz",
            "temperature_c",
            "reject_reason"
        )
    )
