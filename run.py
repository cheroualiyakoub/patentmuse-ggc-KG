import logging
from kb_builder.bigquery_client import BigQueryClient
from kb_builder.neo4j_client    import Neo4jClient
from kb_builder.pipeline        import batch_ingest, prepare_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def start_backfill(total_limit=800_000, chunk_size=500, batch_size=100):
    """
    Process up to `total_limit` unprocessed patents in memory-safe chunks.

    Fetches `chunk_size` patents from BigQuery at a time, writes them to Neo4j
    in `batch_size` sub-batches, marks them as processed, then fetches the next
    chunk — until `total_limit` is reached or nothing remains.

    Args:
        total_limit: Max patents to process in this run.
        chunk_size:  Patents fetched from BigQuery per iteration (controls RAM usage).
        batch_size:  Patents per Neo4j write transaction (controls Neo4j memory).
    """
    bq    = BigQueryClient()
    neo4j = Neo4jClient()

    neo4j.setup_constraints()

    log.info(f"🚀 Starting pipeline — target={total_limit}, chunk={chunk_size}, batch={batch_size}")

    total_processed = 0
    iteration = 0

    while total_processed < total_limit:
        iteration += 1
        remaining   = total_limit - total_processed
        fetch_count = min(chunk_size, remaining)
        log.info(f"📥 Fetching chunk {iteration} ({fetch_count} patents, {total_processed}/{total_limit} done)...")

        df = bq.fetch_patents(limit=fetch_count)

        if df.empty:
            log.info("✅ No more unprocessed patents — pipeline complete.")
            break

        log.info(f"📦 Chunk {iteration}: {len(df)} patents to process")

        chunk_processed = 0
        failed = False

        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i : i + batch_size]
            prepared_batch = [prepare_row(row) for _, row in batch_df.iterrows()]

            try:
                neo4j.execute_batch(batch_ingest, prepared_batch)

                pub_nums = batch_df["publication_number"].tolist()
                bq.mark_as_processed(pub_nums)

                chunk_processed += len(batch_df)
                total_processed += len(batch_df)
                log.info(f"  ✅ {chunk_processed}/{len(df)} in chunk — {total_processed} total")

            except Exception as e:
                log.error(f"❌ Pipeline halted: {e}")
                failed = True
                break

        if failed:
            break

    neo4j.close()
    log.info(f"🏁 Pipeline finished — {total_processed} patents ingested total.")


if __name__ == "__main__":
    start_backfill(total_limit=300_000, chunk_size=100_000, batch_size=100)
