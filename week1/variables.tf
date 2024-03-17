variable "credentials" {
  description = "My Credentials"
  default     = "./keys/service_acct_creds.json"
}

variable "project" {
  description = "Project"
  default     = "de-zoomcamp-413811"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "demo_dataset_de_zoomcamp_413811"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "demo_bucket_de_zoomcamp_413811"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}