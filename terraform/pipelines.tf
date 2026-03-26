# ---------------------------------------------------------------------------
# SDP (Spark Declaritive Pipelines) — Bronze / Silver / Gold medallion layers
# Mirrors resources/lufthansa_pipeline.yml in the DAB bundle
# ---------------------------------------------------------------------------

resource "databricks_pipeline" "lufthansa_bronze_pipeline" {
  name       = "[${var.environment}] Lufthansa Bronze"
  catalog    = var.catalog
  target     = "lufthansa_bronze"
  serverless = true
  continuous = false
  channel    = "PREVIEW"

  library {
    file {
      path = databricks_workspace_file.bronze_ops_ingestion.path
    }
  }

  library {
    file {
      path = databricks_workspace_file.bronze_ref_ingestion.path
    }
  }
}

resource "databricks_pipeline" "lufthansa_silver_pipeline" {
  name       = "[${var.environment}] Lufthansa Silver"
  catalog    = var.catalog
  target     = "lufthansa_silver"
  serverless = true
  continuous = false
  channel    = "PREVIEW"

  library {
    file {
      path = databricks_workspace_file.silver_operations.path
    }
  }

  library {
    file {
      path = databricks_workspace_file.silver_references.path
    }
  }
}

resource "databricks_pipeline" "lufthansa_gold_pipeline" {
  name       = "[${var.environment}] Lufthansa Gold"
  catalog    = var.catalog
  target     = "lufthansa_gold"
  serverless = true
  continuous = false
  channel    = "PREVIEW"

  library {
    file {
      path = databricks_workspace_file.gold_analytics.path
    }
  }
}
