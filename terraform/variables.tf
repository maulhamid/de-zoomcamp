variable "project" {
  description = "GCP project id"
  type        = string
}

variable "region" {
  description = "Default GCP region"
  type        = string
  default     = "asia-southeast2"
}

variable "gcs_bucket_name" {
  description = "Bucket for the TfL raw and silver layers"
  type        = string
}

variable "bigquery_dataset" {
  description = "Dataset for analytics tables"
  type        = string
  default     = "tfl_cycle_analytics"
}

variable "service_account_id" {
  description = "Service account id for pipeline execution"
  type        = string
  default     = "tfl-cycle-pipeline"
}

