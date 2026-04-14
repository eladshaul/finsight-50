variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "The region to deploy resources in"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "The location for BigQuery and Storage"
  type        = string
  default     = "US"
}