output "neo4j_public_ip" {
  description = "Public IP of the Neo4j VM"
  value       = google_compute_address.neo4j_ip.address
}

output "neo4j_browser_url" {
  description = "Neo4j Browser UI URL"
  value       = "http://${google_compute_address.neo4j_ip.address}:7474"
}

output "neo4j_bolt_url" {
  description = "Neo4j Bolt connection string"
  value       = "bolt://${google_compute_address.neo4j_ip.address}:7687"
}

output "neo4j_password_secret" {
  description = "Secret Manager path for Neo4j password"
  value       = google_secret_manager_secret.neo4j_password.name
  sensitive   = true
}

