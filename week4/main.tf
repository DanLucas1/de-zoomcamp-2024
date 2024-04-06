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

resource "google_bigquery_dataset" "ny_taxi_trips" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

resource "google_compute_address" "default" {
  name = "ny-taxi-vm-ip"
}

resource "google_compute_instance" "ny_taxi_vm" {
  name         = "de-zoomcamp-vm"
  machine_type = "e2-standard-4"
  zone         = "us-central1-a"

  # tags = ["placeholder1", "placeholder2"]

  boot_disk {
    auto_delete = false
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.default.address
    }
  }

  metadata = {
    ssh-keys = "${var.ssh_user}:${file(var.public_ssh_key_path)}"
  }

  metadata_startup_script = file(var.startup_script)
}