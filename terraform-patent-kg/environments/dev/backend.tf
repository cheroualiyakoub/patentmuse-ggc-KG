terraform {
  backend "gcs" {
    bucket = "patentmuse-kg-dev-tfstate"
    prefix = "dev/terraform.tfstate"
  }
}