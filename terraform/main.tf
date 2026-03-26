data "databricks_current_user" "me" {}

locals {
  bundle_path = "/Workspace/Users/${data.databricks_current_user.me.user_name}/lufthansa_bundle"
}

# ---------------------------------------------------------------------------
# Workspace file uploads — mirrors the DAB bundle's src/ directory
# ---------------------------------------------------------------------------

resource "databricks_workspace_file" "fetch_all_references" {
  source = "${path.module}/../lufthansa_bundle/src/ingestion/fetch_all_references.py"
  path   = "${local.bundle_path}/src/ingestion/fetch_all_references.py"
}

resource "databricks_workspace_file" "fetch_departures_from_airport" {
  source = "${path.module}/../lufthansa_bundle/src/ingestion/fetch_departures_from_airport.py"
  path   = "${local.bundle_path}/src/ingestion/fetch_departures_from_airport.py"
}

resource "databricks_workspace_file" "bronze_ops_ingestion" {
  source = "${path.module}/../lufthansa_bundle/src/bronze/ops_ingestion.py"
  path   = "${local.bundle_path}/src/bronze/ops_ingestion.py"
}

resource "databricks_workspace_file" "bronze_ref_ingestion" {
  source = "${path.module}/../lufthansa_bundle/src/bronze/ref_ingestion.py"
  path   = "${local.bundle_path}/src/bronze/ref_ingestion.py"
}

resource "databricks_workspace_file" "silver_operations" {
  source = "${path.module}/../lufthansa_bundle/src/silver/silver_operations.py"
  path   = "${local.bundle_path}/src/silver/silver_operations.py"
}

resource "databricks_workspace_file" "silver_references" {
  source = "${path.module}/../lufthansa_bundle/src/silver/silver_references.py"
  path   = "${local.bundle_path}/src/silver/silver_references.py"
}

resource "databricks_workspace_file" "gold_analytics" {
  source = "${path.module}/../lufthansa_bundle/src/gold/gold_flight_analytics.py"
  path   = "${local.bundle_path}/src/gold/gold_flight_analytics.py"
}
