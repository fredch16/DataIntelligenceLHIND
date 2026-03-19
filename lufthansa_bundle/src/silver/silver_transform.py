from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dp.materialized_view(
    name="ops_flights_silver", 
    comment="Cleaned, typed, and deduplicated Lufthansa flights"
)
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