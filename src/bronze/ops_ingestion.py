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
