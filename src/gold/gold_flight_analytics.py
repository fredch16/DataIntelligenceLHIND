from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ==========================================
# 1. THE MASTER VIEW (Enriched Flights)
# ==========================================

@dp.materialized_view(name="gold_fact_flights_master")
def gold_flights_master():
	f = spark.table("main.lufthansa_silver.ops_flights_silver")
	air = spark.table("main.lufthansa_silver.ref_airports_silver")
	airline = spark.table("main.lufthansa_silver.ref_airlines_silver")
	aircraft = spark.table("main.lufthansa_silver.ref_aircraft_silver")
	# Assuming you have a countries reference table
	countries = spark.table("main.lufthansa_silver.ref_countries_silver") 

	# 1. Joins
	df = f.alias("f") \
		.join(airline.alias("al"), col("f.op_airline_id") == col("al.airline_id"), "left") \
		.join(air.alias("org"), col("f.origin_iata") == col("org.airport_code"), "left") \
		.join(air.alias("dst"), col("f.dest_iata") == col("dst.airport_code"), "left") \
		.join(aircraft.alias("ac"), col("f.aircraft_code") == col("ac.aircraft_code"), "left") \
		.join(countries.alias("cntry"), col("dst.country_code") == col("cntry.country_code"), "left")

	# 2. Sequential Enrichment (Geography, Aircraft, Status)
	return df \
		.withColumn("origin_hub", col("org.airport_name")) \
		.withColumn("destination", col("dst.airport_name")) \
		.withColumn("country", col("cntry.country_name")) \
		.withColumn("equip_code", coalesce(col("ac.equip_code"), lit("N/A"))) \
		.withColumn("aircraft_name", coalesce(col("ac.aircraft_name"), lit("Unknown Aircraft"))) \
		.withColumn("dep_hour", hour(col("f.sch_dep_utc"))) \
		.withColumn("raw_time_status", col("f.time_status")) \
		.withColumn("operational_status", 
			when(col("f.time_status") == "DL", "Delayed")
			.when(col("f.status_code").isin("DP", "LD", "IP"), "In Flight")
			.when(col("f.status_code") == "CD", "Cancelled")
			.otherwise("On Time")
		) \
		.select(
			col("f.flight_id"),
			col("al.airline_name"),
			col("f.flight_number"),
			"origin_hub",
			"destination",
			"country",
			"equip_code",
			"aircraft_name",
			col("f.sch_dep_utc"),
			"dep_hour",
			"raw_time_status",
			"operational_status",
			col("f.ingested_at")
		)