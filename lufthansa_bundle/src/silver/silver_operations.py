from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# ===========================================================================
# FLIGHTS — STAGING VIEW
# ===========================================================================

@dp.view(name="flights_staged")
def flights_staged():
    """Explodes the bronze flight payload and flattens all fields into typed columns."""
    return (
        spark.table("main.lufthansa_bronze.ops_flights")
        .withColumn("n",             explode_outer(col("payload.FlightStatusResource.Flights.Flight")))
        # Carriers & Assets
        .withColumn("op_airline_id", col("n.OperatingCarrier.AirlineID"))
        .withColumn("mk_airline_id", col("n.MarketingCarrier.AirlineID"))
        .withColumn("flight_number", col("n.OperatingCarrier.FlightNumber").cast("int"))
        .withColumn("aircraft_code", col("n.Equipment.AircraftCode"))
        .withColumn("tail_number",   col("n.Equipment.AircraftRegistration"))
        # Routes
        .withColumn("origin_iata",   col("n.Departure.AirportCode"))
        .withColumn("dest_iata",     col("n.Arrival.AirportCode"))
        # Status
        .withColumn("status_code",   col("n.FlightStatus.Code"))
        .withColumn("time_status",   col("n.Departure.TimeStatus.Code"))
        # UTC Timestamps (format: "2024-01-01T10:30Z")
        .withColumn("sch_dep_utc",   to_timestamp(col("n.Departure.ScheduledTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mm'Z'"))
        .withColumn("sch_arr_utc",   to_timestamp(col("n.Arrival.ScheduledTimeUTC.DateTime"),   "yyyy-MM-dd'T'HH:mm'Z'"))
        .withColumn("act_dep_utc",   to_timestamp(col("n.Departure.ActualTimeUTC.DateTime"),     "yyyy-MM-dd'T'HH:mm'Z'"))
        # Local Timestamps (format: "2024-01-01T10:30")
        .withColumn("sch_dep_local", to_timestamp(col("n.Departure.ScheduledTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm"))
        .withColumn("sch_arr_local", to_timestamp(col("n.Arrival.ScheduledTimeLocal.DateTime"),   "yyyy-MM-dd'T'HH:mm"))
        .withColumn("act_dep_local", to_timestamp(col("n.Departure.ActualTimeLocal.DateTime"),    "yyyy-MM-dd'T'HH:mm"))
        # Metadata
        .withColumn("ingested_at",   col("ingestion_metadata.ingested_at").cast("timestamp"))
        .withColumn("batch_id",      col("ingestion_metadata.batch_id"))
        .withColumn("source_script", col("ingestion_metadata.script_name"))
        .drop("n", "payload", "ingestion_metadata")
    )


# ===========================================================================
# FLIGHTS — CLEAN TABLE
# ===========================================================================

@dp.materialized_view(
    name="ops_flights_silver",
    comment="Cleaned, typed, and deduplicated Lufthansa flights"
)
@dp.expect_or_drop("valid_flight_number",    "flight_number IS NOT NULL")
@dp.expect_or_drop("valid_departure_airport", "origin_iata IS NOT NULL")
@dp.expect_or_drop("valid_arrival_airport",   "dest_iata IS NOT NULL")
def silver_flights():
    window_spec = Window.partitionBy("flight_id").orderBy(col("ingested_at").desc())

    return (
        dp.read("flights_staged")
        .withColumn(
            "flight_id",
            concat_ws("-", col("op_airline_id"), col("flight_number"), to_date(col("sch_dep_utc")))
        )
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


# ===========================================================================
# FLIGHTS — QUARANTINE TABLE
# ===========================================================================

@dp.materialized_view(
    name="ops_flights_quarantine",
    comment="Records that failed flight quality checks (missing numbers, routes, or old dates)"
)
def flights_quarantine():
    return (
        dp.read("flights_staged")
        .filter(
            col("flight_number").isNull() |
            col("origin_iata").isNull() |
            col("dest_iata").isNull() |
            (col("sch_dep_utc") < "2024-01-01")
        )
    )
