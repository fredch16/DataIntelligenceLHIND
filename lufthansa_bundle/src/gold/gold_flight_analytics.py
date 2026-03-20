from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ==========================================
# 1. THE MASTER VIEW (The "Wide" Table)
# ==========================================

@dp.materialized_view(name="gold_fact_flights_master")
def gold_flights_master():
	f = spark.table("main.lufthansa_silver.ops_flights_silver")
	air = spark.table("main.lufthansa_silver.ref_airports_silver")
	airline = spark.table("main.lufthansa_silver.ref_airlines_silver")

	return f.alias("f") \
		.join(airline.alias("al"), col("f.op_airline_id") == col("al.airline_id"), "left") \
		.join(air.alias("org"), col("f.origin_iata") == col("org.airport_code"), "left") \
		.join(air.alias("dst"), col("f.dest_iata") == col("dst.airport_code"), "left") \
		.select(
			col("f.flight_id"),
			col("al.airline_name"),
			col("f.flight_number"),
			col("org.airport_name").alias("origin_hub"),
			col("dst.airport_name").alias("destination"),
			col("f.sch_dep_utc"),
			hour(col("f.sch_dep_utc")).alias("dep_hour"),
			# Simplified Status for CEO: 
			# 'DL' -> Delayed, 'DP'/'LD' -> Active, 'SCH' -> Scheduled
			when(col("f.status_code") == "DL", "Delayed")
			.when(col("f.status_code").isin("DP", "LD", "IP"), "In Flight")
			.when(col("f.status_code") == "CD", "Cancelled")
			.otherwise("Scheduled").alias("operational_status"),
			col("f.ingested_at")
		)

# ==========================================
# 2. HOURLY AGGREGATION (The Dashboard Source)
# ==========================================

@dp.materialized_view(name="gold_agg_hub_hourly")
def gold_hub_metrics():
	df = spark.table("main.lufthansa_gold.gold_fact_flights_master")
	
	return df.groupBy(
		window(col("sch_dep_utc"), "1 hour").alias("time_window"),
		col("origin_hub")
	).agg(
		count("flight_id").alias("total_departures"),
		countDistinct("airline_name").alias("active_carriers"),
		# Pivot-style counts for the dashboard
		sum(when(col("operational_status") == "Delayed", 1).otherwise(0)).alias("count_delayed"),
		sum(when(col("operational_status") == "Cancelled", 1).otherwise(0)).alias("count_cancelled")
	).select(
		col("time_window.start").alias("hour_start"),
		"origin_hub",
		"total_departures",
		"active_carriers",
		"count_delayed",
		"count_cancelled"
	)