from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ==========================================
# 1. THE MASTER VIEW (The "Wide" Table)
# ==========================================
@dp.materialized_view(
    name="gold_fact_flights_master",
    comment="Enriched flight records with human-readable names and delay flags"
)
def gold_flights_master():
    # Load all Silver components
    f = spark.table("main.lufthansa_silver.ops_flights_silver")
    air = spark.table("main.lufthansa_silver.ref_airports_silver")
    airline = spark.table("main.lufthansa_silver.ref_airlines_silver")
    aircraft = spark.table("main.lufthansa_silver.ref_aircraft_silver")

    # Join strategy: Left joins ensure we don't lose flights if a reference was quarantined
    return f.alias("f") \
        .join(airline.alias("al"), col("f.op_airline_id") == col("al.airline_id"), "left") \
        .join(air.alias("org"), col("f.origin_iata") == col("org.airport_code"), "left") \
        .join(air.alias("dst"), col("f.dest_iata") == col("dst.airport_code"), "left") \
        .join(aircraft.alias("ac"), col("f.aircraft_code") == col("ac.aircraft_code"), "left") \
        .select(
            col("f.flight_id"),
            col("al.airline_name"),
            col("f.flight_number"),
            col("org.airport_name").alias("origin_airport"),
            col("org.city_code").alias("origin_city"),
            col("dst.airport_name").alias("dest_airport"),
            col("ac.aircraft_name"),
            col("f.sch_dep_utc"),
            # Extract hour for the Week 4 requirement
            hour(col("f.sch_dep_utc")).alias("dep_hour"),
            col("f.status_code"),
            # Business Logic: 1 if delayed (DL), 0 otherwise
            when(col("f.status_code") == "DL", 1).otherwise(0).alias("is_delayed"),
            # Lineage
            col("f.ingested_at")
        )

# ==========================================
# 2. HOURLY AGGREGATION (The Dashboard Source)
# ==========================================
@dp.materialized_view(
    name="gold_agg_hourly_performance",
    comment="Hourly pre-aggregated flight performance metrics"
)
def gold_hourly_metrics():
    # We read from our own Master Gold table to keep logic DRY
    df = spark.table("main.lufthansa_gold.gold_fact_flights_master")
    
    return df.groupBy(
        window(col("sch_dep_utc"), "1 hour").alias("time_window"),
        col("airline_name"),
        col("origin_airport")
    ).agg(
        count("flight_id").alias("total_flights"),
        sum("is_delayed").alias("total_delayed_flights")
    ).withColumn(
        "delay_rate_pct", 
        round((col("total_delayed_flights") / col("total_flights")) * 100, 2)
    ).select(
        col("time_window.start").alias("hour_start"),
        "airline_name",
        "origin_airport",
        "total_flights",
        "total_delayed_flights",
        "delay_rate_pct"
    )