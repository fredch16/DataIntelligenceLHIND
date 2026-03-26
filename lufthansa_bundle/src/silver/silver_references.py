from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# --- HELPER FUNCTIONS ---

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
        df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


# ===========================================================================
# 1. AIRPORTS
# ===========================================================================

@dp.view(name="airports_staged")
def airports_staged():
    """Explodes and flattens the bronze airports payload into typed columns."""
    return (
        spark.table("main.lufthansa_bronze.ref_airports_bronze")
        .withColumn("node",          explode_outer(col("payload.AirportResource.Airports.Airport")))
        .withColumn("airport_code",  col("node.AirportCode"))
        .withColumn("airport_name",  get_en_name("Names"))
        .withColumn("city_code",     col("node.CityCode"))
        .withColumn("country_code",  col("node.CountryCode"))
        .withColumn("latitude",      col("node.Position.Coordinate.Latitude").cast("double"))
        .withColumn("longitude",     col("node.Position.Coordinate.Longitude").cast("double"))
        .withColumn("utc_offset",    col("node.UtcOffset"))
        .withColumn("timezone_id",   col("node.TimeZoneId"))
        .withColumn("location_type", col("node.LocationType"))
        .withColumn("ingested_at",   col("ingestion_metadata.ingested_at").cast("timestamp"))
        .withColumn("batch_id",      col("ingestion_metadata.batch_id"))
        .withColumn("source_script", col("ingestion_metadata.script_name"))
        .drop("node", "payload", "ingestion_metadata")
    )


@dp.materialized_view(name="ref_airports_silver", comment="Unique airports with coordinates and timezones")
@dp.expect_or_drop("valid_iata_code", "length(airport_code) = 3")
@dp.expect_or_drop("has_name", "airport_name IS NOT NULL")
@dp.expect("valid_coords", "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180")
def silver_airports():
    return deduplicate_latest(dp.read("airports_staged"), "airport_code")


@dp.materialized_view(name="ref_airports_quarantine", comment="Airport records that failed quality checks")
def airports_quarantine():
    return (
        dp.read("airports_staged")
        .filter(
            (length(col("airport_code")) != 3) |
            col("airport_code").isNull() |
            col("airport_name").isNull()
        )
    )


# ===========================================================================
# 2. CITIES
# ===========================================================================

@dp.view(name="cities_staged")
def cities_staged():
    """Explodes and flattens the bronze cities payload into typed columns."""
    return (
        spark.table("main.lufthansa_bronze.ref_cities_bronze")
        .withColumn("node",          explode_outer(col("payload.CityResource.Cities.City")))
        .withColumn("city_code",     col("node.CityCode"))
        .withColumn("city_name",     get_en_name("Names"))
        .withColumn("country_code",  col("node.CountryCode"))
        .withColumn("utc_offset",    col("node.UtcOffset"))
        .withColumn("timezone_id",   col("node.TimeZoneId"))
        .withColumn("ingested_at",   col("ingestion_metadata.ingested_at").cast("timestamp"))
        .withColumn("batch_id",      col("ingestion_metadata.batch_id"))
        .withColumn("source_script", col("ingestion_metadata.script_name"))
        .drop("node", "payload", "ingestion_metadata")
    )


@dp.materialized_view(name="ref_cities_silver", comment="Unique cities with country mapping and local offsets")
@dp.expect_or_drop("valid_city_code", "city_code IS NOT NULL")
@dp.expect_or_drop("has_name", "city_name IS NOT NULL")
def silver_cities():
    return deduplicate_latest(dp.read("cities_staged"), "city_code")


@dp.materialized_view(name="ref_cities_quarantine", comment="City records that failed quality checks")
def cities_quarantine():
    return (
        dp.read("cities_staged")
        .filter(
            col("city_code").isNull() |
            col("city_name").isNull()
        )
    )


# ===========================================================================
# 3. COUNTRIES
# ===========================================================================

@dp.view(name="countries_staged")
def countries_staged():
    """Explodes and flattens the bronze countries payload into typed columns."""
    return (
        spark.table("main.lufthansa_bronze.ref_countries_bronze")
        .withColumn("node",          explode_outer(col("payload.CountryResource.Countries.Country")))
        .withColumn("country_code",  col("node.CountryCode"))
        .withColumn("country_name",  get_en_name("Names"))
        .withColumn("ingested_at",   col("ingestion_metadata.ingested_at").cast("timestamp"))
        .withColumn("batch_id",      col("ingestion_metadata.batch_id"))
        .withColumn("source_script", col("ingestion_metadata.script_name"))
        .drop("node", "payload", "ingestion_metadata")
    )


@dp.materialized_view(name="ref_countries_silver", comment="Unique countries mapping codes to English names")
@dp.expect_or_drop("valid_country_code", "country_code IS NOT NULL")
@dp.expect_or_drop("has_name", "country_name IS NOT NULL")
def silver_countries():
    return deduplicate_latest(dp.read("countries_staged"), "country_code")


@dp.materialized_view(name="ref_countries_quarantine", comment="Country records that failed quality checks")
def countries_quarantine():
    return (
        dp.read("countries_staged")
        .filter(
            col("country_code").isNull() |
            col("country_name").isNull()
        )
    )


# ===========================================================================
# 4. AIRLINES
# ===========================================================================

@dp.view(name="airlines_staged")
def airlines_staged():
    """Explodes and flattens the bronze airlines payload into typed columns."""
    return (
        spark.table("main.lufthansa_bronze.ref_airlines_bronze")
        .withColumn("node",          explode_outer(col("payload.AirlineResource.Airlines.Airline")))
        .withColumn("airline_id",    col("node.AirlineID"))
        .withColumn("icao_code",     col("node.AirlineID_ICAO"))
        .withColumn("airline_name",  get_en_name("Names"))
        .withColumn("ingested_at",   col("ingestion_metadata.ingested_at").cast("timestamp"))
        .withColumn("batch_id",      col("ingestion_metadata.batch_id"))
        .withColumn("source_script", col("ingestion_metadata.script_name"))
        .drop("node", "payload", "ingestion_metadata")
    )


@dp.materialized_view(name="ref_airlines_silver", comment="Unique airlines with ICAO and IATA mapping")
@dp.expect_or_drop("valid_airline_id", "airline_id IS NOT NULL")
@dp.expect_or_drop("has_name", "airline_name IS NOT NULL")
def silver_airlines():
    return deduplicate_latest(dp.read("airlines_staged"), "airline_id")


@dp.materialized_view(name="ref_airlines_quarantine", comment="Airline records that failed quality checks")
def airlines_quarantine():
    return (
        dp.read("airlines_staged")
        .filter(
            col("airline_id").isNull() |
            col("airline_name").isNull()
        )
    )


# ===========================================================================
# 5. AIRCRAFT
# ===========================================================================

@dp.view(name="aircraft_staged")
def aircraft_staged():
    """Explodes and flattens the bronze aircraft payload into typed columns."""
    return (
        spark.table("main.lufthansa_bronze.ref_aircraft_bronze")
        .withColumn("node",          explode_outer(col("payload.AircraftResource.AircraftSummaries.AircraftSummary")))
        .withColumn("aircraft_code", col("node.AircraftCode"))
        .withColumn("equip_code",    col("node.AirlineEquipCode"))
        .withColumn("aircraft_name", get_en_name("Names"))
        .withColumn("ingested_at",   col("ingestion_metadata.ingested_at").cast("timestamp"))
        .withColumn("batch_id",      col("ingestion_metadata.batch_id"))
        .withColumn("source_script", col("ingestion_metadata.script_name"))
        .drop("node", "payload", "ingestion_metadata")
    )


@dp.materialized_view(name="ref_aircraft_silver", comment="Unique aircraft codes mapping to equipment types")
@dp.expect_or_drop("valid_aircraft_code", "aircraft_code IS NOT NULL")
@dp.expect_or_drop("has_name", "aircraft_name IS NOT NULL")
def silver_aircraft():
    return deduplicate_latest(dp.read("aircraft_staged"), "aircraft_code")


@dp.materialized_view(name="ref_aircraft_quarantine", comment="Aircraft records that failed quality checks")
def aircraft_quarantine():
    return (
        dp.read("aircraft_staged")
        .filter(
            col("aircraft_code").isNull() |
            col("aircraft_name").isNull()
        )
    )
