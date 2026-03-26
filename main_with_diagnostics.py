import logging
from kb_builder.bigquery_client import BigQueryClient
from kb_builder.neo4j_client    import Neo4jClient
from kb_builder.pipeline        import batch_ingest, prepare_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def start_backfill(total_limit=50, batch_size=100, require_npl=False):
    """
    Run the patent ingestion pipeline.
    
    Args:
        total_limit: Maximum number of patents to process
        batch_size: Number of patents per batch
        require_npl: If True, only process patents with NPL citations
    """
    bq = BigQueryClient()
    neo4j = Neo4jClient()
    
    # 1. Setup Constraints once
    neo4j.setup_constraints()

    log.info(f"🚀 Starting Pipeline: Total Limit {total_limit}, Require NPL: {require_npl}")

    # 2. Fetch data from BigQuery
    df = bq.fetch_patents(limit=total_limit, require_npl=require_npl)
    
    if df.empty:
        log.warning("⚠️  No patents returned!")
        log.info("💡 Suggestions:")
        log.info("   1. Check if all patents are already processed (is_kg_generated = TRUE)")
        log.info("   2. Try: require_npl=False to process patents without NPL")
        log.info("   3. Run: bq.reset_processed_flag(10) to reprocess some patents")
        return

    log.info(f"✅ Processing {len(df)} patents")

    # 3. Process in batches
    total_processed = 0
    
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i : i + batch_size]
        
        # Transform rows for Neo4j UNWIND
        prepared_batch = []
        for idx, row in batch_df.iterrows():
            try:
                prepared = prepare_row(row)
                prepared_batch.append(prepared)
            except Exception as e:
                log.error(f"❌ Failed to prepare row {row['publication_number']}: {e}")
                continue
        
        if not prepared_batch:
            log.warning(f"⚠️  Batch {i//batch_size + 1} has no valid rows, skipping")
            continue
        
        try:
            # Send to Neo4j
            neo4j.execute_batch(batch_ingest, prepared_batch)
            total_processed += len(prepared_batch)
            log.info(f"✅ Neo4j: Ingested batch {i//batch_size + 1} ({len(prepared_batch)} patents)")

            # 4. CHECKPOINT: Mark these IDs as processed in BigQuery
            pub_nums = batch_df["publication_number"].tolist()
            bq.mark_as_processed(pub_nums)
            log.info(f"📍 BigQuery: Marked {len(pub_nums)} patents as processed.")

        except Exception as e:
            log.error(f"❌ Pipeline halted at batch {i//batch_size + 1}: {e}")
            import traceback
            traceback.print_exc()
            break 

    neo4j.close()
    
    log.info("="*80)
    log.info(f"🏁 Pipeline Finished")
    log.info(f"   • Total patents processed: {total_processed}")
    log.info("="*80)


if __name__ == "__main__":
    # First, run diagnostics
    bq = BigQueryClient()
    
    print("\n" + "="*80)
    print("🔍 RUNNING DIAGNOSTICS FIRST")
    print("="*80)
    bq.diagnose_data()
    
    print("\n" + "="*80)
    print("📋 OPTIONS:")
    print("="*80)
    print("1. Process patents WITH NPL only:     require_npl=True")
    print("2. Process ALL unprocessed patents:   require_npl=False")
    print("3. Reset some patents for testing:    bq.reset_processed_flag(10)")
    print("="*80)
    
    # Choose your option:
    
    # Option 1: Only patents with NPL citations
    # start_backfill(total_limit=50, batch_size=100, require_npl=True)
    
    # Option 2: Process ALL unprocessed patents (even without NPL)
    start_backfill(total_limit=50, batch_size=100, require_npl=False)
    
    # Option 3: Reset and reprocess (for testing)
    # bq.reset_processed_flag(10)
    # start_backfill(total_limit=10, batch_size=5, require_npl=False)