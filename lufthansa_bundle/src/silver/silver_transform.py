from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dp.materialized_view(
    name="flights_silver",
    comment="Cleaned, typed, and deduplicated Lufthansa flights (Operational & Timestamps)"
)
def silver_flights():
    # 1. Read the live Bronze table
    raw_df = dp.read("ops_flights_bronze")
    
    # 2. THE ARRAY BUG FIX
    # Lufthansa sometimes sends 1 flight (Object) and sometimes 10 (Array).
    # Spark's `typeof` function checks the structure dynamically. If it's an array, 
    # we leave it. If it's an object, we wrap it in an array() so we can safely explode it.
    safe_flight_col = expr(
        "CASE WHEN typeof(FlightStatusResource.Flights.Flight) LIKE 'array%' "
        "THEN FlightStatusResource.Flights.Flight "
        "ELSE array(FlightStatusResource.Flights.Flight) END"
    )
    
    # 'explode_outer' turns the array into individual rows without dropping empty ones
    exploded_df = raw_df.withColumn("flight_node", explode_outer(safe_flight_col))
    
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
        
        # UTC Timestamps (For Math)
        to_timestamp(col("flight_node.Departure.ScheduledTimeUTC.DateTime")).alias("sch_dep_utc"),
        to_timestamp(col("flight_node.Arrival.ScheduledTimeUTC.DateTime")).alias("sch_arr_utc"),
        to_timestamp(col("flight_node.Departure.ActualTimeUTC.DateTime")).alias("act_dep_utc"),
        
        # Local Timestamps (For Dashboards)
        to_timestamp(col("flight_node.Departure.ScheduledTimeLocal.DateTime")).alias("sch_dep_local"),
        to_timestamp(col("flight_node.Arrival.ScheduledTimeLocal.DateTime")).alias("sch_arr_local"),
        to_timestamp(col("flight_node.Departure.ActualTimeLocal.DateTime")).alias("act_dep_local"),
        
        # Custom Metadata
        col("ingested_at").cast("timestamp").alias("ingested_at"),
        col("batch_id").alias("batch_id"),
        col("_metadata.file_path").alias("source_file")
    )
    
    # 4. PRIMARY KEY GENERATION
    # We create a unique ID by combining Carrier + Flight Number + Scheduled Departure Date
    # e.g., "LH-400-2026-03-18"
    pk_df = flat_df.filter(col("flight_number").isNotNull()).withColumn(
        "flight_id",
        concat_ws("-", col("op_airline_id"), col("flight_number"), to_date(col("sch_dep_utc")))
    )
    
    # 5. DEDUPLICATION (The "Maturity" Logic)
    # If we fetch the same flight twice, we partition by our new flight_id,
    # order them by ingested_at (newest first), and only keep row #1.
    window_spec = Window.partitionBy("flight_id").orderBy(col("ingested_at").desc())
    
    final_silver_df = (
        pk_df.withColumn("row_num", row_number().over(window_spec))
             .filter(col("row_num") == 1)
             .drop("row_num")
    )
    
    return final_silver_df