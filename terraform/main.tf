provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "lake" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true
  storage_class               = "STANDARD"
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = var.bigquery_dataset
  location                   = var.region
  delete_contents_on_destroy = false
}

resource "google_service_account" "pipeline" {
  account_id   = var.service_account_id
  display_name = "TfL Cycle Analytics Pipeline"
}

