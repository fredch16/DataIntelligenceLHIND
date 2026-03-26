from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Helper to create a standardized ingestion function for different reference types
def create_ref_table(ref_name):
	@dp.table(
		name=f"ref_{ref_name}_bronze",
		comment=f"Raw ingestion of {ref_name} reference data"
	)
	def ingest_ref():
		return (
			spark.readStream.format("cloudFiles")
				.option("cloudFiles.format", "json")
				.option("cloudFiles.inferColumnTypes", "true")
				.option("multiLine", "true") 
				.load(f"/Volumes/main/lufthansa/landing_zone/ref/{ref_name}")
		)

# List of all reference datasets
ref_types = ["airports", "countries", "cities", "airlines", "aircraft"]

# Generate the tables
for ref in ref_types:
	create_ref_table(ref)