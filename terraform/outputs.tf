output "lake_bucket_name" {
  value = google_storage_bucket.lake.name
}

output "analytics_dataset_id" {
  value = google_bigquery_dataset.analytics.dataset_id
}

output "service_account_email" {
  value = google_service_account.pipeline.email
}

