provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "finance_dataset" {
  dataset_id = "sp500_top50_analysis_gold"
  location   = var.location 
  delete_contents_on_destroy = true
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-raw-data-lake"
  location      = var.location
  force_destroy = true
  uniform_bucket_level_access = true
}

resource "local_file" "kestra_env" {
  filename = "${path.module}/../.env"
  content  = <<-EOT
    GCP_PROJECT_ID=${var.project_id}
    GCP_REGION=${var.region}
    BQ_DATASET=${google_bigquery_dataset.finance_dataset.dataset_id}
    GCS_BUCKET=${google_storage_bucket.data_lake.name}
  EOT
}