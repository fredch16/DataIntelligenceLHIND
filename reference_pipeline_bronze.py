import dlt
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit

# Configuration
SOURCE_BASE = "/Volumes/main/lufthansa/landing_zone/ref/"

def create_bronze_table(ref_name):
	@dlt.table(
		name=f"ref_{ref_name}_bronze",
		comment=f"Raw Bronze ingestion for Lufthansa {ref_name} reference data.",
		table_properties={
			"quality": "bronze",
			"pipelines.autoOptimize.zOrderCols": "_ingested_at"
		}
	)
	def ingest():
		return (
			spark.readStream.format("cloudFiles")
			.option("cloudFiles.format", "json")
			.option("cloudFiles.inferColumnTypes", "true")
			# DLT handles schema location/checkpoints automatically
			.load(f"{SOURCE_BASE}{ref_name}/*")
			.withColumn("_source_file", input_file_name())
			.withColumn("_ingested_at", current_timestamp())
			.withColumn("_ref_type", lit(ref_name))
		)

# Dynamically generate tables for each reference type
ref_types = ["airports", "countries", "cities", "airlines"]

for ref in ref_types:
	create_bronze_table(ref)