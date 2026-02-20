variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "patentmuse-kg-dev"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west3" # Frankfurt, Germany
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "europe-west3-a" # Frankfurt zone a
}

variable "neo4j_password" {
  description = "Neo4j admin password"
  type        = string
  sensitive   = true
}

variable "machine_type" {
  description = "GCE machine type for Neo4j VM"
  type        = string
  default     = "e2-standard-4" # 4 vCPU, 16GB RAM
}

variable "disk_size_gb" {
  description = "Boot disk size in GB"
  type        = number
  default     = 50 # increase to 100 if needed
}