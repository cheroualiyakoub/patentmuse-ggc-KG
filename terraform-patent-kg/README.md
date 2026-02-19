# terraform-patent-kg

Overview

This project contains the Terraform infrastructure code to deploy and manage the patent knowledge graph system on Google Cloud. The system supports patent professionals in prior art search, claim reasoning, and technology analysis by combining a Neo4j AuraDB knowledge graph with vector search and backend services.

Purpose

The repository enables infrastructure as code (IaC) best practices for provisioning, managing, and scaling all components required for the patent knowledge graph platform. It ensures reproducibility, version control, security, and environment isolation across development, staging, and production deployments.

Key Components Managed

- Network: VPC, subnets, firewall rules, and private connectivity.
- Secrets: Neo4j credentials and service account keys stored securely in Secret Manager.
- Storage: Buckets for raw patents, processed data, and backups.
- Backend Service: Cloud Run or GKE deployment of API services connecting to the knowledge graph.
- Monitoring: Logging, alerts, and metrics collection.

Structure

- `modules/` — reusable Terraform modules (network, storage, Neo4j connectivity, IAM, monitoring).
- `environments/` — separate configurations for `dev`, `staging`, and `prod`.
- `global/` — provider configuration and Terraform version management.
- `.github/workflows/` — CI/CD pipeline for plan and apply automation.

Best Practices Followed

- Environment isolation (dev, staging, prod)
- Remote state storage in Google Cloud Storage (GCS)
- Secrets never stored in the repo (use Secret Manager)
- Modular and reusable code
- Version control and CI/CD workflow with GitHub Actions

Goal

Provide a secure, reproducible, and production-ready infrastructure for a patent knowledge graph platform, enabling teams to focus on data modeling, graph reasoning, and analytics rather than manual cloud management.

Getting started (quick)

1. Clone the repo and open `terraform-patent-kg/`.
2. Configure your Google Cloud credentials and project.
3. Configure the remote backend (GCS bucket) and service account permissions.
4. From an environment folder (e.g. `environments/dev`) run:

```bash
terraform init
terraform plan
terraform apply
```

Notes

- Secrets (Neo4j credentials, service account JSON) must be stored in Secret Manager and referenced at runtime. Do not commit secrets or `.tfvars` files.
- Fill in provider and backend configuration in `global/providers.tf` and `global/versions.tf` before running.

Repository layout

```
terraform-patent-kg/
├── environments/
│   ├── dev/
│   ├── staging/
│   └── prod/
├── modules/
│   ├── network/
│   ├── secrets/
│   ├── storage/
│   ├── backend_service/
│   └── monitoring/
├── global/
│   ├── providers.tf
│   └── versions.tf
├── .github/workflows/
│   └── terraform.yml
├── README.md
└── .gitignore
```

Contributing

- Follow the repository conventions for modules and environment-specific overrides.
- Add unit tests or example usages for modules when possible.
- Use branches and open PRs for changes. CI will run `terraform fmt`, `terraform validate`, and `plan`.

Support

If you need help bootstrapping the cloud project, setting up remote state, or writing the first module, open an issue or contact the infra owner.
