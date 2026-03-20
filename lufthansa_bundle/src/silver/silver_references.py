from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# --- 🛠️ HELPER FUNCTIONS ---

def get_en_name(path):
	"""Plucks the English name from the Lufthansa 'Names' structure."""
	return col(f"node.{path}.Name.$")

def deduplicate_latest(df, pk_col):
	"""
	Ensures each Primary Key is unique by plucking the latest 
	record based on the ingestion timestamp.
	"""
	window_spec = Window.partitionBy(pk_col).orderBy(col("ingested_at").desc())
	
	return (
		df.withColumn("row_num", row_number().over(window_spec)) \
			.filter(col("row_num") == 1) \
			.drop("row_num")
	)

# --- 🏛️ DIMENSION TABLES ---

# ==========================================
# 1. AIRPORTS
# ==========================================

@dp.materialized_view(name="ref_airports_silver", comment="Unique airports with coordinates and timezones")
@dp.expect_or_drop("valid_iata_code", "length(airport_code) = 3")
@dp.expect_or_drop("has_name", "airport_name IS NOT NULL")
@dp.expect("valid_coords", "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180")
def silver_airports():
	df = spark.table("main.lufthansa_bronze.ref_airports_bronze")
	nodes = df.withColumn("node", explode_outer(col("payload.AirportResource.Airports.Airport")))
	
	flat_df = nodes.select(
		col("node.AirportCode").alias("airport_code"),
		get_en_name("Names").alias("airport_name"),
		col("node.CityCode").alias("city_code"),
		col("node.CountryCode").alias("country_code"),
		col("node.Position.Coordinate.Latitude").cast("double").alias("latitude"),
		col("node.Position.Coordinate.Longitude").cast("double").alias("longitude"),
		col("node.UtcOffset").alias("utc_offset"),
		col("node.TimeZoneId").alias("timezone_id"),
		col("node.LocationType").alias("location_type"),
		# Metadata for deduplication
		col("ingestion_metadata.ingested_at").cast("timestamp").alias("ingested_at"),
		col("ingestion_metadata.batch_id").alias("batch_id"),
		col("ingestion_metadata.script_name").alias("source_script")
	)

	final_df = deduplicate_latest(flat_df, "airport_code")
	return final_df

# ==========================================
# 2. CITIES
# ==========================================

@dp.materialized_view(name="ref_cities_silver", comment="Unique cities with country mapping and local offsets")
@dp.expect_or_drop("valid_city_code", "city_code IS NOT NULL")
@dp.expect_or_drop("has_name", "city_name IS NOT NULL")
def silver_cities():
	df = spark.table("main.lufthansa_bronze.ref_cities_bronze")
	nodes = df.withColumn("node", explode_outer(col("payload.CityResource.Cities.City")))
	
	flat_df = nodes.select(
		col("node.CityCode").alias("city_code"),
		get_en_name("Names").alias("city_name"),
		col("node.CountryCode").alias("country_code"),
		col("node.UtcOffset").alias("utc_offset"),
		col("node.TimeZoneId").alias("timezone_id"),
		col("ingestion_metadata.ingested_at").cast("timestamp").alias("ingested_at"),
		col("ingestion_metadata.batch_id").alias("batch_id"),
		col("ingestion_metadata.script_name").alias("source_script")
	)
	final_df = deduplicate_latest(flat_df, "city_code")
	return final_df

# ==========================================
# 3. COUNTRIES
# ==========================================

@dp.materialized_view(name="ref_countries_silver", comment="Unique countries mapping codes to English names")
@dp.expect_or_drop("valid_country_code", "country_code IS NOT NULL")
@dp.expect_or_drop("has_name", "country_name IS NOT NULL")
def silver_countries():
	df = spark.table("main.lufthansa_bronze.ref_countries_bronze")
	nodes = df.withColumn("node", explode_outer(col("payload.CountryResource.Countries.Country")))
	
	flat_df = nodes.select(
		col("node.CountryCode").alias("country_code"),
		get_en_name("Names").alias("country_name"),
		col("ingestion_metadata.ingested_at").cast("timestamp").alias("ingested_at"),
		col("ingestion_metadata.batch_id").alias("batch_id"),
		col("ingestion_metadata.script_name").alias("source_script")
	)
	final_df = deduplicate_latest(flat_df, "country_code")
	return final_df

# ==========================================
# 4. AIRLINES
# ==========================================

@dp.materialized_view(name="ref_airlines_silver", comment="Unique airlines with ICAO and IATA mapping")
@dp.expect_or_drop("valid_airline_id", "airline_id IS NOT NULL")
@dp.expect_or_drop("has_name", "airline_name IS NOT NULL")
def silver_airlines():
	df = spark.table("main.lufthansa_bronze.ref_airlines_bronze")
	nodes = df.withColumn("node", explode_outer(col("payload.AirlineResource.Airlines.Airline")))
	
	flat_df = nodes.select(
		col("node.AirlineID").alias("airline_id"),
		col("node.AirlineID_ICAO").alias("icao_code"),
		get_en_name("Names").alias("airline_name"),
		col("ingestion_metadata.ingested_at").cast("timestamp").alias("ingested_at"),
		col("ingestion_metadata.batch_id").alias("batch_id"),
		col("ingestion_metadata.script_name").alias("source_script")
	)
	final_df = deduplicate_latest(flat_df, "airline_id")
	return final_df

# ==========================================
# 5. AIRCRAFT
# ==========================================

@dp.materialized_view(name="ref_aircraft_silver", comment="Unique aircraft codes mapping to equipment types")
@dp.expect_or_drop("valid_aircraft_code", "aircraft_code IS NOT NULL")
@dp.expect_or_drop("has_name", "aircraft_name IS NOT NULL")
def silver_aircraft():
	df = spark.table("main.lufthansa_bronze.ref_aircraft_bronze")
	nodes = df.withColumn("node", explode_outer(col("payload.AircraftResource.AircraftSummaries.AircraftSummary")))
	
	flat_df = nodes.select(
		col("node.AircraftCode").alias("aircraft_code"),
		col("node.AirlineEquipCode").alias("equip_code"),
		get_en_name("Names").alias("aircraft_name"),
		col("ingestion_metadata.ingested_at").cast("timestamp").alias("ingested_at"),
		col("ingestion_metadata.batch_id").alias("batch_id"),
		col("ingestion_metadata.script_name").alias("source_script")
	)
	final_df = deduplicate_latest(flat_df, "aircraft_code")
	return final_df

# --- AIRPORTS QUARANTINE ---
@dp.materialized_view(name="ref_airports_quarantine")
def airports_quarantine():
	df = spark.table("main.lufthansa_bronze.ref_airports_bronze")
	nodes = df.withColumn("node", explode_outer(col("payload.AirportResource.Airports.Airport")))
	
	return nodes.select(
		col("node.AirportCode").alias("airport_code"),
		get_en_name("Names").alias("airport_name"),
		col("ingestion_metadata.batch_id").alias("batch_id")
	).filter("length(airport_code) != 3 OR airport_code IS NULL OR airport_name IS NULL")

# --- CITIES QUARANTINE ---
@dp.materialized_view(name="ref_cities_quarantine")
def cities_quarantine():
	df = spark.table("main.lufthansa_bronze.ref_cities_bronze")
	nodes = df.withColumn("node", explode_outer(col("payload.CityResource.Cities.City")))
	
	return nodes.select(
		col("node.CityCode").alias("city_code"),
		get_en_name("Names").alias("city_name"),
		col("ingestion_metadata.batch_id").alias("batch_id")
	).filter("length(city_code) != 3 OR city_code IS NULL")

# --- COUNTRIES QUARANTINE ---
@dp.materialized_view(name="ref_countries_quarantine")
def countries_quarantine():
	df = spark.table("main.lufthansa_bronze.ref_countries_bronze")
	nodes = df.withColumn("node", explode_outer(col("payload.CountryResource.Countries.Country")))
	
	return nodes.select(
		col("node.CountryCode").alias("country_code"),
		get_en_name("Names").alias("country_name"),
		col("ingestion_metadata.batch_id").alias("batch_id")
	).filter("length(country_code) != 2 OR country_code IS NULL")

# --- AIRLINES QUARANTINE ---
@dp.materialized_view(name="ref_airlines_quarantine")
def airlines_quarantine():
	df = spark.table("main.lufthansa_bronze.ref_airlines_bronze")
	nodes = df.withColumn("node", explode_outer(col("payload.AirlineResource.Airlines.Airline")))
	
	return nodes.select(
		col("node.AirlineID").alias("airline_id"),
		get_en_name("Names").alias("airline_name"),
		col("ingestion_metadata.batch_id").alias("batch_id")
	).filter("airline_id IS NULL OR airline_name IS NULL")

# --- AIRCRAFT QUARANTINE ---
@dp.materialized_view(name="ref_aircraft_quarantine")
def aircraft_quarantine():
	df = spark.table("main.lufthansa_bronze.ref_aircraft_bronze")
	nodes = df.withColumn("node", explode_outer(col("payload.AircraftResource.AircraftSummaries.AircraftSummary")))
	
	return nodes.select(
		col("node.AircraftCode").alias("aircraft_code"),
		get_en_name("Names").alias("aircraft_name"),
		col("ingestion_metadata.batch_id").alias("batch_id")
	).filter("aircraft_code IS NULL OR aircraft_name IS NULL")