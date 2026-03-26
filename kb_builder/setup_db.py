import logging
from kb_builder.neo4j_client import Neo4jClient

log = logging.getLogger(__name__)


def setup():
    client = Neo4jClient()
    try:
        client.verify()
        client.setup_constraints()
        log.info("Database setup complete.")
    finally:
        client.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    setup()