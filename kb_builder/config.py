import os
from dotenv import load_dotenv

load_dotenv()

# Neo4j
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

# BigQuery
GCP_PROJECT_ID    = "patentmuse-kg-dev"
BQ_DATASET        = "ipable_patents_private_us"
BQ_PATENTS_TABLE  = "patents_20250901"
PUBLIC_PATENTS_TABLE = "patents-public-data.patents.publications"
BQ_CLAIMS_TABLE   = "claims"

# --- Pipeline Settings ---
BATCH_SIZE = 100
QUERY_LOCATION = "US"  # Crucial for cross-region performance