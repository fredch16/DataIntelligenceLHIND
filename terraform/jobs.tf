# ---------------------------------------------------------------------------
# Jobs — mirrors resources/lufthansa_jobs.yml in the DAB bundle
# ---------------------------------------------------------------------------

# Job 1: Monthly reference data ingestion
# Schedule: 01:55 UTC on the 3rd of every month
resource "databricks_job" "ingest_reference_data_monthly" {
  name = "Ingest reference data (Monthly)"

  schedule {
    quartz_cron_expression = "0 55 1 3 * ?"
    timezone_id            = "UTC"
    pause_status           = "UNPAUSED"
  }

  environment {
    environment_key = "Default"
    spec {
      client = "4"
    }
  }

  task {
    task_key        = "fetch_reference_data_api"
    environment_key = "Default"

    spark_python_task {
      python_file = databricks_workspace_file.fetch_all_references.path
    }
  }

  queue {
    enabled = true
  }
}

# ---------------------------------------------------------------------------

# Job 2: Utility — ad-hoc operational ingestion (no schedule)
resource "databricks_job" "utility_ingest_operational" {
  name = "Utility Operational Ingestion"

  environment {
    environment_key = "Default"
    spec {
      client = "4"
    }
  }

  task {
    task_key        = "fetch_api_data"
    environment_key = "Default"

    spark_python_task {
      python_file = databricks_workspace_file.fetch_departures_from_airport.path
    }
  }

  email_notifications {
    on_failure = [var.notification_email]
  }

  queue {
    enabled = true
  }
}

# ---------------------------------------------------------------------------

# Job 3: Daily operational pipeline — API → Bronze → Silver → Gold
# Schedule: every 4 hours UTC, skipping midnight (04:00, 08:00, 12:00, 16:00, 20:00)
resource "databricks_job" "ingest_operational_data_daily" {
  name = "Ingest Operational Data (Daily)"

  schedule {
    quartz_cron_expression = "0 0 4,8,12,16,20 * * ?"
    timezone_id            = "UTC"
    pause_status           = "UNPAUSED"
  }

  environment {
    environment_key = "Default"
    spec {
      client = "4"
    }
  }

  # Task 1: Pull flight data from the Lufthansa API
  task {
    task_key        = "fetch_api_data"
    environment_key = "Default"

    spark_python_task {
      python_file = databricks_workspace_file.fetch_departures_from_airport.path
    }
  }

  # Task 2: Ingest raw JSON into the Bronze DLT layer
  task {
    task_key = "refresh_bronze"

    depends_on {
      task_key = "fetch_api_data"
    }

    pipeline_task {
      pipeline_id = databricks_pipeline.lufthansa_bronze_pipeline.id
    }
  }

  # Task 3: Clean and type the Bronze data into the Silver layer
  task {
    task_key = "refresh_silver"

    depends_on {
      task_key = "refresh_bronze"
    }

    pipeline_task {
      pipeline_id = databricks_pipeline.lufthansa_silver_pipeline.id
    }
  }

  # Task 4: Build the enriched Gold analytics fact table
  task {
    task_key = "refresh_gold"

    depends_on {
      task_key = "refresh_silver"
    }

    pipeline_task {
      pipeline_id = databricks_pipeline.lufthansa_gold_pipeline.id
    }
  }

  email_notifications {
    on_failure = [var.notification_email]
  }

  queue {
    enabled = true
  }
}
