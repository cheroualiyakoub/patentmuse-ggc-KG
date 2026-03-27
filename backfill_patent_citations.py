import logging
import numpy as np
from kb_builder.bigquery_client import BigQueryClient
from kb_builder.neo4j_client import Neo4jClient
from kb_builder.pipeline import batch_ingest_patent_citations

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def backfill(chunk_size=100_000, batch_size=500):
    """
    One-time backfill: add patent-to-patent CITES_PATENT relationships
    for all already-processed patents.

    Cost-efficient:
        - chunk_size=100K → ~5 BigQuery calls for 500K patents
        - MERGE is idempotent — safe to re-run if interrupted
        - batch_size=500 for Neo4j writes (simple Cypher, can handle larger batches)

    Progress tracking:
        - Uses OFFSET pagination in BigQuery to avoid re-processing
        - If interrupted, re-running skips already-MERGEd relationships (idempotent)
    """
    bq = BigQueryClient()
    neo4j = Neo4jClient()

    log.info(f"Starting patent citation backfill (chunk={chunk_size}, batch={batch_size})")

    total_patents = 0
    total_relationships = 0
    offset = 0

    while True:
        log.info(f"Fetching chunk (offset={offset}, limit={chunk_size})...")

        df = bq.fetch_patent_citations(limit=chunk_size, offset=offset)

        if df.empty:
            log.info("No more patents with citations — backfill complete.")
            break

        chunk_rels = 0

        # Process in batches for Neo4j
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i : i + batch_size]

            # Prepare batch
            prepared = []
            for _, row in batch_df.iterrows():
                citations = row['patent_citations']
                if isinstance(citations, np.ndarray):
                    citations = citations.tolist()

                clean_citations = []
                for c in citations:
                    if isinstance(c, dict):
                        cited_pub = c.get('cited_pub', '')
                        if cited_pub:
                            clean_citations.append({
                                'cited_pub': cited_pub,
                                'category': c.get('category') or '',
                            })

                if clean_citations:
                    prepared.append({
                        'publication_number': row['publication_number'],
                        'patent_citations': clean_citations,
                    })

            if prepared:
                try:
                    summary = neo4j.execute_batch(batch_ingest_patent_citations, prepared)
                    chunk_rels += summary.counters.relationships_created
                except Exception as e:
                    log.error(f"Batch failed: {e}")
                    neo4j.close()
                    return

            processed = min(i + batch_size, len(df))
            log.info(f"  {processed}/{len(df)} in chunk — {total_patents + processed} patents total")

        total_patents += len(df)
        total_relationships += chunk_rels
        offset += len(df)

        log.info(f"Chunk done: {chunk_rels} new relationships. Running total: {total_relationships}")

    neo4j.close()
    log.info(f"Backfill complete: {total_patents} patents, {total_relationships} CITES_PATENT relationships created.")


if __name__ == "__main__":
    backfill(chunk_size=100_000, batch_size=500)
