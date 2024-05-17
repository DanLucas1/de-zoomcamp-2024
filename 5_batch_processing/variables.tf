variable "credentials" {
  description = "My Credentials"
  default     = "./keys/service_acct_creds.json"
}

variable "public_ssh_key_path" {
  description = "Path to the public SSH key to be used for VM access"
  default     = "~/.ssh/gcloud_key.pub"
}

variable "ssh_user" {
  description = "Username for SSH access"
  default     = "dan.r.lucas"
}

variable "project" {
  description = "Project"
  default     = "de-zoomcamp-413811"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "us-central1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "ny_taxi_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "ny_taxi_storage_413811"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "startup_script" {
  description = "VM startup script"
  default     = "./startup.sh"
}

variable "zoomcamp_service_account_email" {
  description = "email linked to gcloud project service account - reads from tfvars"
  type        = string
}