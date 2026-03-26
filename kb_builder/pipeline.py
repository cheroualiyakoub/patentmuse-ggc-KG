# kb_builder/pipeline.py

from kb_builder.npl_parser import NPLParser
import logging
from typing import Dict, List
import numpy as np

log = logging.getLogger(__name__)

def prepare_row(row) -> Dict:
    """
    Transform a BigQuery row into Neo4j-ready format with NPL parsing.
    
    Args:
        row: Pandas DataFrame row
        
    Returns:
        dict: Prepared data for Neo4j ingestion
    """
    # Extract English title and abstract
    title_en = None
    abstract_en = None
    
    # Handle title_localized (can be None, list, or numpy array)
    title_localized = row.get("title_localized")
    if title_localized is not None:
        # Convert to list if it's a numpy array
        if isinstance(title_localized, np.ndarray):
            title_localized = title_localized.tolist()
        
        if isinstance(title_localized, list) and len(title_localized) > 0:
            for t in title_localized:
                if isinstance(t, dict) and t.get("language") == "en":
                    title_en = t.get("text")
                    break
    
    # Handle abstract_localized
    abstract_localized = row.get("abstract_localized")
    if abstract_localized is not None:
        if isinstance(abstract_localized, np.ndarray):
            abstract_localized = abstract_localized.tolist()
        
        if isinstance(abstract_localized, list) and len(abstract_localized) > 0:
            for a in abstract_localized:
                if isinstance(a, dict) and a.get("language") == "en":
                    abstract_en = a.get("text")
                    break
    
    # Extract inventors
    inventors = []
    inventor_harmonized = row.get("inventor_harmonized")
    if inventor_harmonized is not None:
        if isinstance(inventor_harmonized, np.ndarray):
            inventor_harmonized = inventor_harmonized.tolist()
        
        if isinstance(inventor_harmonized, list):
            for inv in inventor_harmonized:
                if isinstance(inv, dict):
                    name = inv.get("name")
                    if name:
                        inventors.append({"name": name})
    
    # Extract assignees
    assignees = []
    assignee_harmonized = row.get("assignee_harmonized")
    if assignee_harmonized is not None:
        if isinstance(assignee_harmonized, np.ndarray):
            assignee_harmonized = assignee_harmonized.tolist()
        
        if isinstance(assignee_harmonized, list):
            for asg in assignee_harmonized:
                if isinstance(asg, dict):
                    name = asg.get("name")
                    if name:
                        assignees.append({"name": name})
    
    # Extract IPC codes
    ipc_codes = []
    ipc = row.get("ipc")
    if ipc is not None:
        if isinstance(ipc, np.ndarray):
            ipc = ipc.tolist()
        
        if isinstance(ipc, list):
            for ipc_entry in ipc:
                if isinstance(ipc_entry, dict):
                    code = ipc_entry.get("code")
                    if code:
                        ipc_codes.append(code)
    
    # ============================================================
    # CRITICAL: Parse NPL Citations to Extract Authors
    # ============================================================
    npl_citations = []
    
    npl_citations_raw = row.get("npl_citations")
    if npl_citations_raw is not None:
        # Convert numpy array to list
        if isinstance(npl_citations_raw, np.ndarray):
            npl_citations_raw = npl_citations_raw.tolist()
        
        if isinstance(npl_citations_raw, list):
            for npl_text in npl_citations_raw:
                # Skip empty or invalid entries
                if not npl_text or not isinstance(npl_text, str) or npl_text.strip() == "":
                    continue
                
                # Parse the NPL citation
                parsed = NPLParser.parse_npl(npl_text)
                
                # Only add if we extracted at least one author
                if parsed['authors'] and len(parsed['authors']) > 0:
                    npl_citations.append({
                        'authors': parsed['authors'],
                        'title': parsed['title'] or npl_text[:100],  # Fallback to raw text
                        'year': parsed['year'],
                        'raw_text': npl_text,
                        'full_reference': npl_text  # Used as unique identifier
                    })
                    
                    log.debug(f"📚 Parsed NPL: {len(parsed['authors'])} authors from: {npl_text[:60]}...")
                else:
                    log.debug(f"⚠️  No authors found in: {npl_text[:60]}...")
    
    # Log statistics
    pub_num = row.get("publication_number", "UNKNOWN")
    if npl_citations:
        total_authors = sum(len(npl['authors']) for npl in npl_citations)
        log.debug(f"✅ Patent {pub_num}: {len(npl_citations)} NPL citations, {total_authors} total authors")
    else:
        log.debug(f"ℹ️  Patent {pub_num}: No NPL citations with authors")
    
    return {
        "publication_number": str(row.get("publication_number", "")),
        "title": title_en or "No Title",
        "abstract": abstract_en or "",
        "publication_date": str(row.get("publication_date", "")),
        "filing_date": str(row.get("filing_date", "")),
        "country_code": str(row.get("country_code", "")),
        "family_id": str(row.get("family_id", "")),
        "inventors": inventors,
        "assignees": assignees,
        "ipc_codes": ipc_codes,
        "npl_citations": npl_citations,
    }


def batch_ingest(tx, batch: List[Dict]):
    """
    Neo4j transaction function to ingest a batch of patents with NPL citations.
    
    This creates:
    - Patent nodes
    - Inventor nodes + INVENTED_BY relationships
    - Assignee nodes + ASSIGNED_TO relationships
    - IPC classification nodes + hierarchy
    - Article nodes (from NPL) + CITES relationships
    - Author nodes + WRITTEN_BY relationships
    """
    
    cypher = """
    UNWIND $batch AS patent
    
    // 1. Create Patent node
    MERGE (p:Patent {publication_number: patent.publication_number})
    SET p.title = patent.title,
        p.abstract = patent.abstract,
        p.publication_date = patent.publication_date,
        p.filing_date = patent.filing_date,
        p.country_code = patent.country_code
    
    // 2. Create Family node
    WITH p, patent
    MERGE (fam:Family {family_id: patent.family_id})
    MERGE (p)-[:BELONGS_TO_FAMILY]->(fam)
    
    // 3. Create Inventors
    WITH p, patent
    FOREACH (inv IN patent.inventors |
        MERGE (i:Inventor {name: inv.name})
        MERGE (i)-[:INVENTED]->(p)
    )
    
    // 4. Create Assignees
    WITH p, patent
    FOREACH (asg IN patent.assignees |
        MERGE (a:Assignee {name: asg.name})
        MERGE (p)-[:ASSIGNED_TO]->(a)
    )
    
    // 5. Create IPC codes and hierarchy
    WITH p, patent
    FOREACH (ipc_code IN patent.ipc_codes |
        // Create all levels of IPC hierarchy
        MERGE (sec:IPC_Section {code: substring(ipc_code, 0, 1)})
        MERGE (cls:IPC_Class {code: substring(ipc_code, 0, 3)})
        MERGE (sub:IPC_Subclass {code: substring(ipc_code, 0, 4)})
        MERGE (grp:IPC_Group {code: ipc_code})
        
        // Build hierarchy
        MERGE (cls)-[:PART_OF]->(sec)
        MERGE (sub)-[:PART_OF]->(cls)
        MERGE (grp)-[:PART_OF]->(sub)
        
        // Link patent to finest-grain IPC
        MERGE (p)-[:CLASSIFIED_AS]->(grp)
    )
    
    
    // 6. Create NPL Articles with Authors
    WITH p, patent
    FOREACH (npl IN patent.npl_citations |
        // Create Article node
        MERGE (art:Article {full_reference: npl.full_reference})
        SET art.title = npl.title,
            art.year = npl.year,
            art.raw_text = npl.raw_text
        
        // Link patent to article
        MERGE (p)-[:CITES]->(art)

        // Create Author nodes and WROTE relationships
        FOREACH (author_name IN npl.authors |
            MERGE (author:Author {name: author_name})
            MERGE (author)-[:WROTE]->(art)
        )
    )
    """
    
    try:
        result = tx.run(cypher, batch=batch)
        summary = result.consume()
        
        # Log statistics
        log.info(f"Neo4j Batch Stats:")
        log.info(f"  • Nodes created: {summary.counters.nodes_created}")
        log.info(f"  • Relationships created: {summary.counters.relationships_created}")
        log.info(f"  • Properties set: {summary.counters.properties_set}")
        
        return summary
        
    except Exception as e:
        log.error(f"❌ Cypher execution failed: {e}")
        log.error(f"First patent in batch: {batch[0]['publication_number'] if batch else 'N/A'}")
        raise