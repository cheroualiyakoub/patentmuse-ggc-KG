Modules for terraform-patent-kg

Each module should be self-contained and expose variables for configuration and outputs for consumption by environment stacks.

Suggested modules:
- `network/` - VPC, subnets, firewall rules
- `secrets/` - Secret Manager wiring for Neo4j and service accounts
- `storage/` - GCS buckets for raw, processed and backup data
- `backend_service/` - Cloud Run or GKE service definition
- `monitoring/` - Logging, metrics, alerting

Place module variables, README examples, and a minimal `main.tf`/`outputs.tf`/`variables.tf` inside each module folder.