from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
	name="ops_flights",
	comment="Incremental ingestion of flight data"
)
def ingest_flights():
	return (
		spark.readStream.format("cloudFiles")
			.option("cloudFiles.format", "json")
			.option("multiLine", "true")
			.option("cloudFiles.inferColumnTypes", "true")
			.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
			.load("/Volumes/main/lufthansa/landing_zone/ops/flights")
	)

# # 2. THE MATERIALIZED VIEW (Silver Transformation)
# @dp.materialized_view(
# 	name="flights_silver"
# )
# def clean_flights():
# 	# Lakeflow manages the batch refresh automatically!
# 	return spark.read.table("LIVE.ops_flights_bronze").select(
# 		col("flight_id"),
# 		col("departure_time").cast("timestamp"),
# 		col("status")
# 	)