terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


# CREATE CLOUD STORAGE BUCKET
resource "google_storage_bucket" "ny_taxi_storage" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# CREATE BIGQUERY DATASET
resource "google_bigquery_dataset" "ny_taxi_trips" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

# CREATE SIMPLE DATAPROC CLUSTER
resource "google_dataproc_cluster" "de-zoomcamp-cluster" {
  name   = "de-zoomcamp-cluster"
  region = "us-central1"

  cluster_config {
    staging_bucket = var.gcs_bucket_name

    endpoint_config {
      enable_http_port_access = true
    }

    gce_cluster_config {
      service_account        = var.zoomcamp_service_account_email
      service_account_scopes = ["cloud-platform"]
    }
  }
}