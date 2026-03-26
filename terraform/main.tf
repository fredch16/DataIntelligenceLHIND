data "databricks_current_user" "me" {}

locals {
  project_root = "/Workspace/Users/${data.databricks_current_user.me.user_name}"
}

# ---------------------------------------------------------------------------
# Workspace file uploads — mirrors the DAB bundle's src/ directory
# ---------------------------------------------------------------------------

resource "databricks_workspace_file" "fetch_all_references" {
  source = "${path.module}./src/ingestion/fetch_all_references.py"
  path   = "${local.project_root}/src/ingestion/fetch_all_references.py"
}

resource "databricks_workspace_file" "fetch_departures_from_airport" {
  source = "${path.module}./src/ingestion/fetch_departures_from_airport.py"
  path   = "${local.project_root}/src/ingestion/fetch_departures_from_airport.py"
}

resource "databricks_workspace_file" "bronze_ops_ingestion" {
  source = "${path.module}./src/bronze/ops_ingestion.py"
  path   = "${local.project_root}/src/bronze/ops_ingestion.py"
}

resource "databricks_workspace_file" "bronze_ref_ingestion" {
  source = "${path.module}./src/bronze/ref_ingestion.py"
  path   = "${local.project_root}/src/bronze/ref_ingestion.py"
}

resource "databricks_workspace_file" "silver_operations" {
  source = "${path.module}./src/silver/silver_operations.py"
  path   = "${local.project_root}/src/silver/silver_operations.py"
}

resource "databricks_workspace_file" "silver_references" {
  source = "${path.module}./src/silver/silver_references.py"
  path   = "${local.project_root}/src/silver/silver_references.py"
}

resource "databricks_workspace_file" "gold_analytics" {
  source = "${path.module}./src/gold/gold_flight_analytics.py"
  path   = "${local.project_root}/src/gold/gold_flight_analytics.py"
}

resource "databricks_workspace_file" "utils_helpers" {
  source = "${path.module}./src/utils/helpers.py"
  path   = "${local.project_root}/src/utils/helpers.py"
}
