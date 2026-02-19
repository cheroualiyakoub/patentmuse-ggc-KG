// Placeholder providers.tf
// Add provider configuration and backend (GCS) here. Example:

provider "google" {
  project = var.project_id
  region  = var.region
}

// remote state backend (example - update bucket and prefix per environment):
// terraform {
//   backend "gcs" {
//     bucket  = "your-terraform-state-bucket"
//     prefix  = "terraform/patent-kg"
//   }
// }
