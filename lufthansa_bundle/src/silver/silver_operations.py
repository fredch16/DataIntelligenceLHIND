from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dp.materialized_view(
    name="ops_flights_silver", 
    comment="Cleaned, typed, and deduplicated Lufthansa flights"
)
@dp.expect_or_drop("valid_flight_number", "flight_number IS NOT NULL")
@dp.expect_or_drop("valid_departure_airport", "origin_iata IS NOT NULL")
@dp.expect_or_drop("valid_arrival_airport", "dest_iata IS NOT NULL")
def silver_flights():
    # 1. Read from the Unity Catalog Bronze table
    raw_df = spark.table("main.lufthansa_bronze.ops_flights") 
    
    # 2. EXPLODE (Auto Loader already made it an Array for us!)
    exploded_df = raw_df.withColumn(
        "flight_node", 
        explode_outer(col("payload.FlightStatusResource.Flights.Flight"))
    )
    
    # 3. FLATTEN AND TYPE CASTING
    flat_df = exploded_df.select(
        # Carriers & Assets
        col("flight_node.OperatingCarrier.AirlineID").alias("op_airline_id"),
        col("flight_node.MarketingCarrier.AirlineID").alias("mk_airline_id"),
        col("flight_node.OperatingCarrier.FlightNumber").cast("int").alias("flight_number"),
        col("flight_node.Equipment.AircraftCode").alias("aircraft_code"),
        col("flight_node.Equipment.AircraftRegistration").alias("tail_number"),
        
        # Routes
        col("flight_node.Departure.AirportCode").alias("origin_iata"),
        col("flight_node.Arrival.AirportCode").alias("dest_iata"),
        
        # Status
        col("flight_node.FlightStatus.Code").alias("status_code"),
        col("flight_node.Departure.TimeStatus.Code").alias("time_status"),
        
		# UTC Timestamps (Explicitly handling the 'T' and 'Z' without seconds)
        to_timestamp(col("flight_node.Departure.ScheduledTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mm'Z'").alias("sch_dep_utc"),
        to_timestamp(col("flight_node.Arrival.ScheduledTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mm'Z'").alias("sch_arr_utc"),
        to_timestamp(col("flight_node.Departure.ActualTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mm'Z'").alias("act_dep_utc"),
        
        # Local Timestamps (Explicitly handling the 'T' without seconds)
        to_timestamp(col("flight_node.Departure.ScheduledTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm").alias("sch_dep_local"),
        to_timestamp(col("flight_node.Arrival.ScheduledTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm").alias("sch_arr_local"),
        to_timestamp(col("flight_node.Departure.ActualTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm").alias("act_dep_local"),
        
        # Metadata
        col("ingestion_metadata.ingested_at").cast("timestamp").alias("ingested_at"),
        col("ingestion_metadata.batch_id").alias("batch_id"),
        col("ingestion_metadata.script_name").alias("source_script")
    )
    
    # 4. PRIMARY KEY GENERATION
    pk_df = flat_df.filter(col("flight_number").isNotNull()).withColumn(
        "flight_id",
        concat_ws("-", col("op_airline_id"), col("flight_number"), to_date(col("sch_dep_utc")))
    )
    
    # 5. DEDUPLICATION
    window_spec = Window.partitionBy("flight_id").orderBy(col("ingested_at").desc())
    
    final_silver_df = (
        pk_df.withColumn("row_num", row_number().over(window_spec))
             .filter(col("row_num") == 1)
             .drop("row_num")
    )
    
    return final_silver_df

@dp.materialized_view(
    name="ops_flights_quarantine",
    comment="Records that failed flight quality checks (Missing numbers, routes, or old dates)"
)
def flights_quarantine():
    df = spark.table("main.lufthansa_bronze.ops_flights")
    
    # Flatten just enough to check the rules
    exploded = df.withColumn("flight_node", explode_outer(col("payload.FlightStatusResource.Flights.Flight")))
    
    flat = exploded.select(
        col("flight_node.OperatingCarrier.FlightNumber").alias("flight_number"),
        col("flight_node.Departure.AirportCode").alias("origin_iata"),
        col("flight_node.Arrival.AirportCode").alias("dest_iata"),
        to_timestamp(col("flight_node.Departure.ScheduledTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mm'Z'").alias("sch_dep_utc"),
        col("ingestion_metadata.batch_id").alias("batch_id")
    )

    # CATCH THE REJECTS: The "OR" logic captures anything that failed ANY rule
    return flat.filter(
        (col("flight_number").isNull()) |
        (col("origin_iata").isNull()) |
        (col("dest_iata").isNull()) |
        (col("sch_dep_utc") < '2024-01-01')
    )