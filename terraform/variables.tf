variable "databricks_host" {
  type        = string
  description = "The Databricks workspace URL"
}

variable "databricks_token" {
  type        = string
  description = "The Databricks Personal Access Token"
  sensitive   = true
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev or prod)"
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Environment must be 'dev' or 'prod'."
  }
}

variable "catalog" {
  type        = string
  description = "Unity Catalog name"
  default     = "main"
}

variable "notification_email" {
  type        = string
  description = "Email address for job failure notifications"
  default     = "frederickcharbonnier@icloud.com"
}
