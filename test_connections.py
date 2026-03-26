from kb_builder.neo4j_client import Neo4jClient
from kb_builder.bigquery_client import BigQueryClient


def test_neo4j():
    print("\nTesting Neo4j...")
    client = Neo4jClient()
    try:
        client.verify()
        result = client.run_query("RETURN 'Neo4j is working!' AS message")
        print(result)
    finally:
        client.close()

def test_bigquery():
    print("\nTesting BigQuery...")
    client = BigQueryClient()
    df = client.fetch_patents(limit=2)
    print("\nParsed titles:")
    print(df[['publication_number', 'title', 'abstract']].to_string())

if __name__ == "__main__":
    test_neo4j()
    test_bigquery()