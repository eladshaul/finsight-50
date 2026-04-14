output "bigquery_dataset_id" {
  description = "The ID of the BigQuery dataset created"
  value       = google_bigquery_dataset.finance_dataset.dataset_id
}


output "gcs_bucket_name" {
  description = "The name of the Google Storage Bucket"
  value       = google_storage_bucket.data_lake.name
}


output "project_id" {
  value = var.project_id
}