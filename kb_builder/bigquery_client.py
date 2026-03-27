# kb_builder/bigquery_client.py

from google.cloud import bigquery
from kb_builder.config import GCP_PROJECT_ID, BQ_DATASET, BQ_PATENTS_TABLE, PUBLIC_PATENTS_TABLE
import logging

log = logging.getLogger(__name__)

class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=GCP_PROJECT_ID)
    
    def diagnose_data(self):
        """
        Run diagnostics to understand why no patents are being fetched.
        """
        queries = {
            "Total patents in index": f"""
                SELECT COUNT(*) as count
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}`
            """,
            
            "Unprocessed patents": f"""
                SELECT COUNT(*) as count
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}`
                WHERE is_kg_generated = FALSE
            """,
            
            "Sample citation structure": f"""
                SELECT 
                    idx.publication_number,
                    pub.citation
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}` AS idx
                JOIN `{PUBLIC_PATENTS_TABLE}` AS pub
                    ON idx.publication_number = pub.publication_number
                WHERE idx.is_kg_generated = FALSE
                LIMIT 1
            """,
            
            "Patents with ANY citations": f"""
                SELECT COUNT(*) as count
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}` AS idx
                JOIN `{PUBLIC_PATENTS_TABLE}` AS pub
                    ON idx.publication_number = pub.publication_number
                WHERE 
                    idx.is_kg_generated = FALSE
                    AND ARRAY_LENGTH(pub.citation) > 0
            """,
            
            "Sample patent with citations": f"""
                SELECT 
                    idx.publication_number,
                    ARRAY_LENGTH(pub.citation) as citation_count,
                    (SELECT cit.npl_text FROM UNNEST(pub.citation) AS cit LIMIT 1) as sample_npl
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}` AS idx
                JOIN `{PUBLIC_PATENTS_TABLE}` AS pub
                    ON idx.publication_number = pub.publication_number
                WHERE 
                    idx.is_kg_generated = FALSE
                    AND ARRAY_LENGTH(pub.citation) > 0
                LIMIT 5
            """,
        }
        
        log.info("Running diagnostics...")
        for name, query in queries.items():
            try:
                result = self.client.query(query).to_dataframe()
                log.info(f"{name}:\n{result}")
            except Exception as e:
                log.error(f"{name}: {e}")
    
    def fetch_patents(self, limit: int = 100, offset: int = 0, require_npl: bool = False):
        """
        Fetches patent details with NPL citations properly extracted.
        
        Args:
            limit: Number of patents to fetch
            offset: Offset for pagination
            require_npl: If True, only return patents with NPL citations
        """
        
        # Properly handle STRUCT citation field
        query = f"""
            WITH npl_extracted AS (
                SELECT
                    idx.publication_number,
                    pub.title_localized,
                    pub.abstract_localized,
                    pub.publication_date,
                    pub.filing_date,
                    pub.inventor_harmonized,
                    pub.assignee_harmonized,
                    pub.ipc,
                    pub.country_code,
                    pub.family_id,
                    
                    -- Extract NPL citations from STRUCT array
                    ARRAY(
                        SELECT cit.npl_text
                        FROM UNNEST(pub.citation) AS cit
                        WHERE cit.npl_text IS NOT NULL
                          AND cit.npl_text != ''
                          AND LENGTH(cit.npl_text) > 10  -- Filter out garbage
                    ) AS npl_citations,

                    -- Extract patent-to-patent citations
                    ARRAY(
                        SELECT AS STRUCT
                            cit.publication_number AS cited_pub,
                            cit.category
                        FROM UNNEST(pub.citation) AS cit
                        WHERE cit.publication_number IS NOT NULL
                          AND cit.publication_number != ''
                    ) AS patent_citations,

                    -- Count total citations for debugging
                    ARRAY_LENGTH(pub.citation) as total_citations
                    
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}` AS idx
                JOIN `{PUBLIC_PATENTS_TABLE}` AS pub
                    ON idx.publication_number = pub.publication_number
                WHERE 
                    idx.is_kg_generated = FALSE
                    AND EXISTS (
                        SELECT 1 
                        FROM UNNEST(pub.title_localized) AS t 
                        WHERE t.language = 'en'
                    )
            )
            SELECT
                publication_number,
                title_localized,
                abstract_localized,
                publication_date,
                filing_date,
                inventor_harmonized,
                assignee_harmonized,
                ipc,
                country_code,
                family_id,
                npl_citations,
                patent_citations,
                ARRAY_LENGTH(npl_citations) as npl_count,
                total_citations
            FROM npl_extracted
            {f"WHERE ARRAY_LENGTH(npl_citations) > 0" if require_npl else ""}
            ORDER BY total_citations DESC NULLS LAST  -- Prioritize patents with citations
            LIMIT {limit} OFFSET {offset}
        """
        
        log.info(f"📊 Fetching {limit} patents (offset: {offset}, require_npl: {require_npl})")
        
        try:
            query_job = self.client.query(query)
            df = query_job.to_dataframe()
            
            if df.empty:
                log.warning("⚠️  No patents returned. Running diagnostics...")
                self.diagnose_data()
                return df
            
            log.info(f"✅ Fetched {len(df)} patents")
            
            # Calculate statistics
            patents_with_npl = (df['npl_count'] > 0).sum()
            total_npl = df['npl_count'].sum()
            total_citations = df['total_citations'].sum()
            
            log.info(f"📖 Patents with NPL citations: {patents_with_npl}/{len(df)}")
            log.info(f"📖 Total NPL citations: {total_npl}")
            log.info(f"📊 Total citations (all types): {total_citations}")
            
            # Show sample
            if len(df) > 0:
                # Find a patent with NPL if available
                sample_idx = df['npl_count'].idxmax() if patents_with_npl > 0 else 0
                sample = df.loc[sample_idx]
                
                log.info(f"\n📄 Sample Patent: {sample['publication_number']}")
                log.info(f"   Total citations: {sample['total_citations']}")
                log.info(f"   NPL citations: {sample['npl_count']}")
                
                if sample['npl_count'] > 0:
                    log.info(f"   First NPL: {sample['npl_citations'][0][:100]}...")
                else:
                    log.info(f"   ⚠️  This patent has no NPL citations")
            
            return df
            
        except Exception as e:
            log.error(f"❌ BigQuery fetch failed: {e}")
            raise
    
    def mark_as_processed(self, publication_numbers: list):
        """Mark patents as processed in your index table."""
        if not publication_numbers:
            return
        
        # Create a comma-separated list of quoted publication numbers
        pub_nums_str = ", ".join([f"'{pn}'" for pn in publication_numbers])
        
        query = f"""
            UPDATE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}`
            SET is_kg_generated = TRUE
            WHERE publication_number IN ({pub_nums_str})
        """
        
        try:
            query_job = self.client.query(query)
            query_job.result()  # Wait for completion
            log.info(f"✅ Marked {len(publication_numbers)} patents as processed")
        except Exception as e:
            log.error(f"❌ Failed to mark patents as processed: {e}")
            raise
    
    def reset_processed_flag(self, limit: int = 10):
        """
        UTILITY: Reset is_kg_generated flag for testing.
        Use this to re-process patents.
        """
        query = f"""
            UPDATE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}`
            SET is_kg_generated = FALSE
            WHERE is_kg_generated = TRUE
            LIMIT {limit}
        """
        
        try:
            self.client.query(query).result()
            log.info(f"Reset {limit} patents for reprocessing")
        except Exception as e:
            log.error(f"❌ Failed to reset flags: {e}")
            raise
    
    def fetch_patent_citations(self, limit: int = 100_000, offset: int = 0):
        """
        Fetch patent-to-patent citations for all processed patents.
        Returns publication_number + array of {cited_pub, category} structs.

        Cost-efficient: only fetches citation links, no full patent data.
        Uses large chunks to minimize BigQuery scan costs.
        """
        query = f"""
            SELECT
                idx.publication_number,
                ARRAY(
                    SELECT AS STRUCT
                        cit.publication_number AS cited_pub,
                        cit.category
                    FROM UNNEST(pub.citation) AS cit
                    WHERE cit.publication_number IS NOT NULL
                      AND cit.publication_number != ''
                ) AS patent_citations
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}` AS idx
            JOIN `{PUBLIC_PATENTS_TABLE}` AS pub
                ON idx.publication_number = pub.publication_number
            WHERE idx.is_kg_generated = TRUE
              AND EXISTS (
                  SELECT 1 FROM UNNEST(pub.citation) AS cit
                  WHERE cit.publication_number IS NOT NULL
                    AND cit.publication_number != ''
              )
            ORDER BY idx.publication_number
            LIMIT {limit} OFFSET {offset}
        """

        log.info(f"📊 Fetching patent citations (limit: {limit}, offset: {offset})")

        try:
            df = self.client.query(query).to_dataframe()
            if not df.empty:
                total_citations = df['patent_citations'].apply(len).sum()
                log.info(f"✅ Fetched {len(df)} patents with {total_citations} patent citations total")
            else:
                log.info("✅ No more patents with citations to process")
            return df
        except Exception as e:
            log.error(f"❌ Patent citations fetch failed: {e}")
            raise

    def get_npl_statistics(self):
        """
        Get statistics about NPL coverage in your dataset.
        """
        query = f"""
            WITH stats AS (
                SELECT
                    COUNT(*) as total_patents,
                    SUM(CASE WHEN ARRAY_LENGTH(pub.citation) > 0 THEN 1 ELSE 0 END) as patents_with_citations,
                    SUM(CASE 
                        WHEN EXISTS(
                            SELECT 1 FROM UNNEST(pub.citation) AS cit 
                            WHERE cit.npl_text IS NOT NULL AND cit.npl_text != ''
                        ) THEN 1 ELSE 0 
                    END) as patents_with_npl
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}` AS idx
                JOIN `{PUBLIC_PATENTS_TABLE}` AS pub
                    ON idx.publication_number = pub.publication_number
                WHERE idx.is_kg_generated = FALSE
            )
            SELECT 
                total_patents,
                patents_with_citations,
                patents_with_npl,
                ROUND(100.0 * patents_with_citations / total_patents, 2) as pct_with_citations,
                ROUND(100.0 * patents_with_npl / total_patents, 2) as pct_with_npl
            FROM stats
        """
        
        try:
            result = self.client.query(query).to_dataframe()
            log.info(f"NPL Statistics:\n{result.to_string(index=False)}")
            return result
        except Exception as e:
            log.error(f"Statistics query failed: {e}")
            raise