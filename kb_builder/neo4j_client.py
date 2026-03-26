from neo4j import GraphDatabase
from kb_builder.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
import logging

log = logging.getLogger(__name__)

class Neo4jClient:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
            connection_timeout=10,
            # Max retry time helps the VM handle trans-Atlantic lag
            max_transaction_retry_time=30.0 
        )

    def close(self):
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()

    def verify(self):
        """Verify the connection to Neo4j is live."""
        self.driver.verify_connectivity()
        log.info("Neo4j connection verified.")

    # --- Use this for Ingestion ---
    def execute_batch(self, batch_func, data):
        """
        Wrapper for execute_write. 
        batch_func: The function containing your Cypher (e.g., batch_ingest)
        data: The list of prepared patent dictionaries
        """
        with self.driver.session() as session:
            try:
                # This is where the magic happens
                return session.execute_write(batch_func, data)
            except Exception as e:
                log.error(f"Permanent transaction failure: {e}")
                raise

    # --- Keep this for simple READS or metadata checks ---
    def run_query(self, query: str, params: dict = None):
        with self.driver.session() as session:
            return session.run(query, params or {}).data()
    
    def setup_constraints(self):
        """Create indexes and constraints for the Patent KG."""
        commands = [
            # --- Uniqueness Constraints (Critical for MERGE) ---
            "CREATE CONSTRAINT patent_pub_num IF NOT EXISTS FOR (p:Patent) REQUIRE p.publication_number IS UNIQUE",
            "CREATE CONSTRAINT family_id IF NOT EXISTS FOR (f:Family) REQUIRE f.family_id IS UNIQUE",
            "CREATE CONSTRAINT inventor_name IF NOT EXISTS FOR (i:Inventor) REQUIRE i.name IS UNIQUE",
            "CREATE CONSTRAINT assignee_name IF NOT EXISTS FOR (a:Assignee) REQUIRE a.name IS UNIQUE",
            
            # Article and Author (Used by our Super-Regex Fallback)
            "CREATE CONSTRAINT article_ref IF NOT EXISTS FOR (art:Article) REQUIRE art.full_reference IS UNIQUE",
            "CREATE CONSTRAINT author_name IF NOT EXISTS FOR (au:Author) REQUIRE au.name IS UNIQUE",
            
            # --- IPC Hierarchy Constraints ---
            "CREATE CONSTRAINT ipc_sec IF NOT EXISTS FOR (s:IPC_Section) REQUIRE s.code IS UNIQUE",
            "CREATE CONSTRAINT ipc_cls IF NOT EXISTS FOR (c:IPC_Class) REQUIRE c.code IS UNIQUE",
            "CREATE CONSTRAINT ipc_sub IF NOT EXISTS FOR (s:IPC_Subclass) REQUIRE s.code IS UNIQUE",
            "CREATE CONSTRAINT ipc_grp IF NOT EXISTS FOR (g:IPC_Group) REQUIRE g.code IS UNIQUE",

            # --- Search Indexes ---
            "CREATE INDEX patent_title_idx IF NOT EXISTS FOR (p:Patent) ON (p.title)",
            "CREATE INDEX article_title_idx IF NOT EXISTS FOR (art:Article) ON (art.title)",
            "CREATE INDEX country_code_idx IF NOT EXISTS FOR (p:Patent) ON (p.country_code)",
            "CREATE INDEX author_name_search IF NOT EXISTS FOR (au:Author) ON (au.name)"
        ]
        
        with self.driver.session() as session:
            for cmd in commands:
                try:
                    session.run(cmd)
                    log.info(f"✅ Executed: {cmd[:60]}...")
                except Exception as e:
                    log.error(f"❌ Constraint failure on [{cmd[:30]}...]: {e}")
                    
        log.info("All graph constraints and indexes applied.")