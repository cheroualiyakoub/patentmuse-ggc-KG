def fetch_patents(self, limit: int = 100, offset: int = 0):
        """
        Combines 3-way join: Tracking Index + Public Patent Data + PatCit NPL.
        """
        query = f"""
            SELECT
                idx.publication_number,
                pub.title_localized,
                pub.abstract_localized,
                pub.publication_date,
                pub.filing_date,
                pub.inventor_harmonized,
                pub.assignee_harmonized,
                pub.ipc,
                pub.citation,
                pub.country_code,
                pub.family_id,
                -- PatCit Enriched Metadata
                npl.patcit_id,
                npl.title AS patcit_title,
                npl.DOI,
                npl.journal_title,
                npl.date AS patcit_date,
                ARRAY(
                    SELECT AS STRUCT 
                        TRIM(CONCAT(IFNULL(auth.given, ''), ' ', IFNULL(auth.family, ''))) AS full_name 
                    FROM UNNEST(npl.author) AS auth
                ) AS clean_authors
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PATENTS_TABLE}` AS idx
            JOIN `{PUBLIC_PATENTS_TABLE}` AS pub
                ON idx.publication_number = pub.publication_number
            LEFT JOIN (
                -- Flattened PatCit subquery (The Browser-Validated Logic)
                SELECT 
                    cb.publication_number AS join_pub_num,
                    n.* FROM `patcit-public-data.frontpage.bibliographical_reference` AS n,
                UNNEST(n.cited_by) AS cb
            ) AS npl
                ON idx.publication_number = npl.join_pub_num
            WHERE 
                idx.is_kg_generated = FALSE
                AND EXISTS (
                    SELECT 1 
                    FROM UNNEST(pub.title_localized) AS t 
                    WHERE t.language = 'en'
                )
            ORDER BY idx.publication_number
            LIMIT {limit} OFFSET {offset}
        """

        log.info(f"🚀 Executing 3-Way Join for {limit} patents...")
        
        job_config = bigquery.QueryJobConfig() 
        query_job = self.client.query(
            query, 
            job_config=job_config, 
            location=QUERY_LOCATION 
        )

        df = query_job.to_dataframe()
        
        # Apply your existing Python cleaning helpers
        df["title"]    = df["title_localized"].apply(self._extract_english)
        df["abstract"] = df["abstract_localized"].apply(self._extract_english)

        print(f"✅ Surgically fetched {len(df)} enriched patents via PatCit JOIN")
        return df