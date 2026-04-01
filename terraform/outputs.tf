output "bronze_pipeline_id" {
  description = "ID of the Lufthansa Bronze SDP pipeline"
  value       = databricks_pipeline.lufthansa_bronze_pipeline.id
}

output "silver_pipeline_id" {
  description = "ID of the Lufthansa Silver SDP pipeline"
  value       = databricks_pipeline.lufthansa_silver_pipeline.id
}

output "gold_pipeline_id" {
  description = "ID of the Lufthansa Gold SDP pipeline"
  value       = databricks_pipeline.lufthansa_gold_pipeline.id
}

output "job_monthly_reference_id" {
  description = "ID of the monthly reference data ingestion job"
  value       = databricks_job.ingest_reference_data_monthly.id
}

output "job_utility_operational_id" {
  description = "ID of the utility (ad-hoc) operational ingestion job"
  value       = databricks_job.utility_ingest_operational.id
}

output "job_daily_operational_id" {
  description = "ID of the daily operational pipeline job"
  value       = databricks_job.ingest_operational_data_daily.id
}

output "workspace_bundle_path" {
  description = "Workspace path where source files are deployed"
  value       = local.project_root
}
