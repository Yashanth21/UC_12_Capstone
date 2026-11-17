# =====================================================================
# UC-12 Grid Sensor Anomaly Watch - FILE 2 of 2: SILVER -> GOLD
# Add this file to the SAME pipeline as 01_bronze_to_silver.py.
# Creates: gold_anomaly_readings, gold_anomaly_windows, gold_asset_health
# (Metric views for these three tables are in 03_gold_metric_views.sql)
# Every table has a table comment and every column has a column comment.
# =====================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

CATALOG = "uc_grid_sensor"
SILVER = f"{CATALOG}.grid_silver"   # cleaned data (produced by file 1)
GOLD   = f"{CATALOG}.grid_gold"     # business-ready data


# =====================================================================
# STEP 1: SCORE EVERY READING (threshold rules + rolling statistics)
# =====================================================================

@dp.temporary_view()
def telemetry_scored():
    t = spark.read.table(f"{SILVER}.silver_grid_telemetry").alias("t")
    r = spark.read.table(f"{SILVER}.silver_asset_registry").alias("r")

    # POINT-IN-TIME join with the SCD2 registry: each reading gets the
    # registry version that was valid AT THAT MOMENT.
    joined = t.join(
        r,
        (F.col("t.asset_id") == F.col("r.asset_id"))
        & (F.col("t.event_ts") >= F.col("r.__START_AT"))
        & (F.col("r.__END_AT").isNull() | (F.col("t.event_ts") < F.col("r.__END_AT"))),
        "left",
    ).select(
        "t.*", "r.substation_name", "r.rated_capacity_mva",
        "r.firmware_version", "r.maintenance_status",
    )

    # Rolling statistics: compare each reading with the previous 60 readings
    # of the SAME asset (current row excluded).
    w = Window.partitionBy("asset_id").orderBy("event_ts").rowsBetween(-60, -1)
    df = joined
    for c in ["voltage_kv", "frequency_hz", "temperature_c"]:
        mean, sd, n = F.avg(c).over(w), F.stddev(c).over(w), F.count(c).over(w)
        # z-score = how many standard deviations away from this asset's recent normal
        df = df.withColumn(f"z_{c}", F.when((n >= 20) & (sd > 0), (F.col(c) - mean) / sd))
    stat_flag = F.coalesce(
        (F.abs("z_voltage_kv") > 4) | (F.abs("z_frequency_hz") > 4) | (F.abs("z_temperature_c") > 4),
        F.lit(False),
    )

    # Threshold rules
    #  voltage     warn <10.5 / >11.5   critical <10 / >12
    #  frequency   warn <49.5 / >50.5   critical <49 / >51
    #  temperature differs by asset type (see below)
    def yes(cond):               # NULL counts as "no problem"
        return F.coalesce(cond, F.lit(False))

    temp_warn = (F.when(F.col("asset_type") == "Generator", 95)
                  .when(F.col("asset_type") == "Transformer", 85).otherwise(60))
    temp_crit = (F.when(F.col("asset_type") == "Generator", 110)
                  .when(F.col("asset_type") == "Transformer", 100).otherwise(75))

    v, f, x = F.col("voltage_kv"), F.col("frequency_hz"), F.col("temperature_c")
    v_crit, v_warn = yes((v < 10.0) | (v > 12.0)), yes((v < 10.5) | (v > 11.5))
    f_crit, f_warn = yes((f < 49.0) | (f > 51.0)), yes((f < 49.5) | (f > 50.5))
    t_crit, t_warn = yes(x > temp_crit), yes(x > temp_warn)

    return (
        df
        .withColumn("severity",
            F.when(v_crit | f_crit | t_crit, "CRITICAL")
             .when(v_warn | f_warn | t_warn | stat_flag, "WARNING")
             .otherwise("NORMAL"))
        .withColumn("severity_rank",
            F.when(F.col("severity") == "CRITICAL", 2)
             .when(F.col("severity") == "WARNING", 1).otherwise(0))
        .withColumn("triggered_metrics", F.concat_ws(", ",
            F.when(v_warn | v_crit | yes(F.abs("z_voltage_kv") > 4), "VOLTAGE"),
            F.when(f_warn | f_crit | yes(F.abs("z_frequency_hz") > 4), "FREQUENCY"),
            F.when(t_warn | t_crit | yes(F.abs("z_temperature_c") > 4), "TEMPERATURE")))
    )


# =====================================================================
# GOLD 1: ANOMALY READINGS (one row per abnormal reading)
# =====================================================================
@dp.materialized_view(
    name=f"{GOLD}.gold_anomaly_readings",
    comment="Gold anomaly table: every telemetry reading flagged WARNING or CRITICAL by threshold rules or rolling statistics, with the asset registry details that were valid at that moment.",
    schema="""
        event_id            STRING    COMMENT 'Unique ID of the flagged telemetry reading',
        asset_id            STRING    COMMENT 'Asset that produced the reading, e.g. TR-002',
        asset_type          STRING    COMMENT 'Equipment type: Generator, Transformer or Circuit Breaker',
        substation_name     STRING    COMMENT 'Substation of the asset at the time of the reading',
        rated_capacity_mva  DOUBLE    COMMENT 'Rated capacity of the asset in MVA at the time of the reading',
        firmware_version    STRING    COMMENT 'Firmware version of the asset at the time of the reading (from SCD2 registry)',
        maintenance_status  STRING    COMMENT 'Asset status at the time of the reading, e.g. ACTIVE or UNDER_MAINTENANCE (from SCD2 registry)',
        event_ts            TIMESTAMP COMMENT 'Time the reading was taken',
        voltage_kv          DOUBLE    COMMENT 'Measured voltage in kV',
        frequency_hz        DOUBLE    COMMENT 'Measured frequency in Hz',
        temperature_c       DOUBLE    COMMENT 'Measured temperature in degrees Celsius',
        triggered_metrics   STRING    COMMENT 'Which measurements were abnormal: VOLTAGE, FREQUENCY and/or TEMPERATURE',
        severity            STRING    COMMENT 'WARNING = outside normal band or statistical outlier; CRITICAL = outside the critical limit',
        severity_rank       INT       COMMENT 'Number form of severity for sorting: 2 = CRITICAL, 1 = WARNING'
    """,
)
def gold_anomaly_readings():
    return (
        spark.read.table("telemetry_scored")
        .filter("severity != 'NORMAL'")
        .select(
            F.col("event_id").cast("string"),
            F.col("asset_id").cast("string"),
            F.col("asset_type").cast("string"),
            F.col("substation_name").cast("string"),
            F.col("rated_capacity_mva").cast("double"),
            F.col("firmware_version").cast("string"),
            F.col("maintenance_status").cast("string"),
            F.col("event_ts"),
            F.col("voltage_kv"),
            F.col("frequency_hz"),
            F.col("temperature_c"),
            F.col("triggered_metrics").cast("string"),
            F.col("severity").cast("string"),
            F.col("severity_rank").cast("int"),
        )
    )


# =====================================================================
# GOLD 2: ANOMALY WINDOWS (one row per asset per 15-minute window)
# Input for the incident summarizer agent: one row = one engineer note
# =====================================================================
@dp.materialized_view(
    name=f"{GOLD}.gold_anomaly_windows",
    comment="One row per asset per 15-minute window that contains anomalies, with worst values and asset context. This is the input for the incident summarizer agent.",
    schema="""
        window_id            STRING    COMMENT 'Unique ID of the window: asset ID plus window start, e.g. TR-002_202610240830',
        asset_id             STRING    COMMENT 'Asset with the anomalies',
        asset_type           STRING    COMMENT 'Equipment type: Generator, Transformer or Circuit Breaker',
        substation_name      STRING    COMMENT 'Substation of the asset',
        rated_capacity_mva   DOUBLE    COMMENT 'Rated capacity of the asset in MVA',
        firmware_version     STRING    COMMENT 'Firmware version of the asset during the window',
        maintenance_status   STRING    COMMENT 'Asset status during the window, e.g. ACTIVE or UNDER_MAINTENANCE',
        window_start         TIMESTAMP COMMENT 'Start of the 15-minute window',
        window_end           TIMESTAMP COMMENT 'End of the 15-minute window',
        anomalous_readings   BIGINT    COMMENT 'Number of WARNING or CRITICAL readings inside the window',
        min_voltage_kv       DOUBLE    COMMENT 'Lowest voltage among the anomalous readings, in kV',
        max_voltage_kv       DOUBLE    COMMENT 'Highest voltage among the anomalous readings, in kV',
        min_frequency_hz     DOUBLE    COMMENT 'Lowest frequency among the anomalous readings, in Hz',
        max_frequency_hz     DOUBLE    COMMENT 'Highest frequency among the anomalous readings, in Hz',
        max_temperature_c    DOUBLE    COMMENT 'Highest temperature among the anomalous readings, in degrees Celsius',
        triggered_metrics    STRING    COMMENT 'Measurements that were abnormal in the window: VOLTAGE, FREQUENCY and/or TEMPERATURE',
        severity             STRING    COMMENT 'Worst severity in the window: CRITICAL if any reading was critical, otherwise WARNING'
    """,
)
def gold_anomaly_windows():
    a = spark.read.table(f"{GOLD}.gold_anomaly_readings")
    return (
        a.groupBy("asset_id", "asset_type", "substation_name", "rated_capacity_mva",
                  "firmware_version", "maintenance_status",
                  F.window("event_ts", "15 minutes").alias("w"))
        .agg(
            F.count("*").alias("anomalous_readings"),
            F.max("severity_rank").alias("max_rank"),
            F.min("voltage_kv").alias("min_voltage_kv"),
            F.max("voltage_kv").alias("max_voltage_kv"),
            F.min("frequency_hz").alias("min_frequency_hz"),
            F.max("frequency_hz").alias("max_frequency_hz"),
            F.max("temperature_c").alias("max_temperature_c"),
            F.array_join(F.sort_array(F.collect_set("triggered_metrics")), " | ").alias("triggered_metrics"),
        )
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .withColumn("severity", F.when(F.col("max_rank") == 2, "CRITICAL").otherwise("WARNING"))
        .withColumn("window_id", F.concat_ws("_", "asset_id", F.date_format("window_start", "yyyyMMddHHmm")))
        # explicit order + types so they match the schema above
        .select(
            F.col("window_id").cast("string"),
            F.col("asset_id").cast("string"),
            F.col("asset_type").cast("string"),
            F.col("substation_name").cast("string"),
            F.col("rated_capacity_mva").cast("double"),
            F.col("firmware_version").cast("string"),
            F.col("maintenance_status").cast("string"),
            F.col("window_start"),
            F.col("window_end"),
            F.col("anomalous_readings").cast("bigint"),
            F.col("min_voltage_kv"),
            F.col("max_voltage_kv"),
            F.col("min_frequency_hz"),
            F.col("max_frequency_hz"),
            F.col("max_temperature_c"),
            F.col("triggered_metrics").cast("string"),
            F.col("severity").cast("string"),
        )
    )


# =====================================================================
# GOLD 3: ASSET HEALTH (one row per asset, for fleet-level questions)
# =====================================================================
@dp.materialized_view(
    name=f"{GOLD}.gold_asset_health",
    comment="One row per asset: reading counts, warning and critical counts, temperature summary and the current registry details. Used for fleet-level questions in Genie.",
    schema="""
        asset_id            STRING  COMMENT 'Unique asset identifier, e.g. CB-001',
        asset_type          STRING  COMMENT 'Equipment type: Generator, Transformer or Circuit Breaker',
        substation_name     STRING  COMMENT 'Substation of the asset (current registry version)',
        rated_capacity_mva  DOUBLE  COMMENT 'Rated capacity in MVA (current registry version)',
        firmware_version    STRING  COMMENT 'Current firmware version',
        maintenance_status  STRING  COMMENT 'Current status, e.g. ACTIVE or UNDER_MAINTENANCE',
        total_readings      BIGINT  COMMENT 'Number of valid readings received from the asset',
        warning_readings    BIGINT  COMMENT 'Number of readings with severity WARNING',
        critical_readings   BIGINT  COMMENT 'Number of readings with severity CRITICAL',
        avg_temperature_c   DOUBLE  COMMENT 'Average temperature across all readings, in degrees Celsius',
        max_temperature_c   DOUBLE  COMMENT 'Highest temperature recorded, in degrees Celsius'
    """,
)
def gold_asset_health():
    scored = spark.read.table("telemetry_scored")
    current = (spark.read.table(f"{SILVER}.silver_asset_registry")
               .filter("__END_AT IS NULL")                      # latest version only
               .select("asset_id", "substation_name", "rated_capacity_mva",
                       "firmware_version", "maintenance_status"))
    stats = scored.groupBy("asset_id", "asset_type").agg(
        F.count("*").alias("total_readings"),
        F.sum(F.when(F.col("severity") == "WARNING", 1).otherwise(0)).alias("warning_readings"),
        F.sum(F.when(F.col("severity") == "CRITICAL", 1).otherwise(0)).alias("critical_readings"),
        F.round(F.avg("temperature_c"), 1).alias("avg_temperature_c"),
        F.max("temperature_c").alias("max_temperature_c"),
    )
    return (
        stats.join(current, "asset_id", "left")
        .select(
            F.col("asset_id").cast("string"),
            F.col("asset_type").cast("string"),
            F.col("substation_name").cast("string"),
            F.col("rated_capacity_mva").cast("double"),
            F.col("firmware_version").cast("string"),
            F.col("maintenance_status").cast("string"),
            F.col("total_readings").cast("bigint"),
            F.col("warning_readings").cast("bigint"),
            F.col("critical_readings").cast("bigint"),
            F.col("avg_temperature_c").cast("double"),
            F.col("max_temperature_c").cast("double"),
        )
    )

# Gold Ai_incident notes

MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
 
# The instructions the model receives for every window.
# We ask for 4 labelled lines (not JSON): models follow this reliably,
# and it is easy to split into columns with a simple pattern.
INSTRUCTIONS = """You are an assistant for grid engineers. Write a short incident note for the anomaly window below.
Use ONLY the facts given. Do not invent numbers.
 
Reply with EXACTLY these four lines, each starting with its label, and nothing else.
Do not use JSON, quotes, bullet points or code fences.
DEVIATION: <one short sentence on how far the worst metric is from nominal, with numbers and units>
CATEGORY: <exactly one of OVERHEATING, VOLTAGE_SAG, VOLTAGE_SURGE, FREQUENCY_DRIFT, SENSOR_FAULT_SUSPECT>
CHECK: <one short sentence telling the engineer what to inspect first>
NOTE: <two short sentences an engineer can read quickly, mentioning the asset, the deviation and the status>
 
Rules:
- Pick CATEGORY from the metric with the largest deviation (voltage below nominal = VOLTAGE_SAG, above = VOLTAGE_SURGE).
- If the asset status is UNDER_MAINTENANCE, say so in NOTE and make CHECK include confirming whether maintenance work explains it.
 
FACTS:"""
 
 
# Small helper: pull "LABEL: text" out of the model's reply.
# Returns NULL (not an empty string) when the label is missing.
def grab(label):
    text = F.trim(F.regexp_extract("raw_response", rf"(?m)^\s*{label}:\s*(.+?)\s*$", 1))
    return F.when(F.length(text) > 0, text)
 
 
@dp.materialized_view(
    name=f"{GOLD}.gold_incident_notes",
    comment="AI-written, engineer-readable incident note for each anomaly window (asset, deviation, likely category, recommended check), generated with ai_query.",
    schema="""
        window_id          STRING    COMMENT 'Anomaly window this note describes; matches gold_anomaly_windows.window_id',
        asset_id           STRING    COMMENT 'Asset with the anomaly',
        asset_type         STRING    COMMENT 'Equipment type: Generator, Transformer or Circuit Breaker',
        substation_name    STRING    COMMENT 'Substation of the asset',
        maintenance_status STRING    COMMENT 'Asset status during the window, e.g. ACTIVE or UNDER_MAINTENANCE',
        window_start       TIMESTAMP COMMENT 'Start of the 15-minute window',
        window_end         TIMESTAMP COMMENT 'End of the 15-minute window',
        severity           STRING    COMMENT 'Worst severity in the window: WARNING or CRITICAL',
        triggered_metrics  STRING    COMMENT 'Measurements that were abnormal in the window',
        deviation          STRING    COMMENT 'AI-written sentence: how far the worst metric is from nominal',
        likely_category    STRING    COMMENT 'AI-chosen category: OVERHEATING, VOLTAGE_SAG, VOLTAGE_SURGE, FREQUENCY_DRIFT or SENSOR_FAULT_SUSPECT',
        recommended_check  STRING    COMMENT 'AI-written first thing for the engineer to inspect',
        note               STRING    COMMENT 'AI-written two-sentence incident note for engineers',
        raw_response       STRING    COMMENT 'Raw model output, kept for debugging; NULL parts in the columns above mean a label was missing from the reply'
    """,
)
def gold_incident_notes():
    w = spark.read.table(f"{GOLD}.gold_anomaly_windows")
 
    # Normal temperature depends on the asset type (same values as the gold scoring)
    nominal_temp = (F.when(F.col("asset_type") == "Generator", 80)
                     .when(F.col("asset_type") == "Transformer", 70).otherwise(45))
 
    # Worst voltage / frequency deviation from nominal (11 kV, 50 Hz).
    # We calculate the numbers here so the model does not have to do maths.
    v_dev = (F.when(F.abs(F.col("min_voltage_kv") - 11.0) > F.abs(F.col("max_voltage_kv") - 11.0),
                    F.col("min_voltage_kv") - 11.0)
              .otherwise(F.col("max_voltage_kv") - 11.0))
    f_dev = (F.when(F.abs(F.col("min_frequency_hz") - 50.0) > F.abs(F.col("max_frequency_hz") - 50.0),
                    F.col("min_frequency_hz") - 50.0)
              .otherwise(F.col("max_frequency_hz") - 50.0))
 
    facts = F.concat_ws("\n",
        F.format_string("Asset: %s (%s) at %s", F.col("asset_id"), F.col("asset_type"), F.col("substation_name")),
        F.format_string("Status during window: %s, firmware %s", F.col("maintenance_status"), F.col("firmware_version")),
        F.format_string("Window: %s to %s, severity %s, %d anomalous readings",
                        F.col("window_start"), F.col("window_end"), F.col("severity"), F.col("anomalous_readings")),
        F.format_string("Triggered metrics: %s", F.col("triggered_metrics")),
        F.format_string("Voltage: min %.2f kV, max %.2f kV (nominal 11 kV, worst deviation %+.2f kV)",
                        F.col("min_voltage_kv"), F.col("max_voltage_kv"), v_dev),
        F.format_string("Frequency: min %.2f Hz, max %.2f Hz (nominal 50 Hz, worst deviation %+.2f Hz)",
                        F.col("min_frequency_hz"), F.col("max_frequency_hz"), f_dev),
        F.format_string("Temperature: max %.1f C (nominal %d C, deviation %+.1f C)",
                        F.col("max_temperature_c"), nominal_temp, F.col("max_temperature_c") - nominal_temp),
    )
 
    df = (
        w
        .withColumn("prompt", F.concat(F.lit(INSTRUCTIONS + "\n"), facts))
        # The AI call: one model request per window. Temperature 0 = steadier answers.
        .withColumn(
            "raw_response",
            F.expr(f"ai_query('{MODEL_ENDPOINT}', prompt, "
                   "modelParameters => named_struct('max_tokens', 300, 'temperature', 0.0))"),
        )
    )
 
    return df.select(
        F.col("window_id").cast("string"),
        F.col("asset_id").cast("string"),
        F.col("asset_type").cast("string"),
        F.col("substation_name").cast("string"),
        F.col("maintenance_status").cast("string"),
        F.col("window_start"),
        F.col("window_end"),
        F.col("severity").cast("string"),
        F.col("triggered_metrics").cast("string"),
        grab("DEVIATION").alias("deviation"),
        F.upper(grab("CATEGORY")).alias("likely_category"),
        grab("CHECK").alias("recommended_check"),
        grab("NOTE").alias("note"),
        F.col("raw_response").cast("string"),
    )
 