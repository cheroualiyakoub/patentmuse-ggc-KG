"""
For each cluster in v3, find the top-10 most-central patents (highest weighted
in-cluster degree across CITES_PATENT + SIMILAR_TITLE + SIMILAR_ABSTRACT edges)
and fetch their full abstracts. This is the "nearest to the centroid" set —
the patents whose connections best represent the cluster's identity.

Output:
    cluster_h02p_v3_out/cluster_naming_data.json
        {
          "coarse_03": [{cluster_id, size, top_central: [{pub, title, abstract,
                         in_cluster_degree, top_assignees, ipc_groups}]}, ...],
          "l2":        [...]
        }

Run:
    /home/yacoubcherouali/kg-builder/venv/bin/python name_clusters_v3.py
"""

import json
import os
import time
from collections import Counter, defaultdict
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
V3_DIR = SCRIPT_DIR / "cluster_h02p_v3_out"
load_dotenv(SCRIPT_DIR / ".env")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

BATCH_SIZE = 5000

# Weight scheme matches the v3 clustering pipeline.
CITE_WEIGHT = 3.0
TITLE_WEIGHT_FACTOR = 0.4
ABSTRACT_WEIGHT_FACTOR = 0.6

MIN_CLUSTER_SIZE_TO_NAME = 20    # only name clusters with >=20 patents
TOP_K = 10                       # top-K central patents per cluster


def fetch_edges_with_score(session, pubs, edge_type, label):
    query = f"""
    UNWIND $batch AS source_pub
    MATCH (s:Patent {{publication_number: source_pub}})-[r:{edge_type}]-(t:Patent)
    RETURN source_pub AS source, t.publication_number AS target, r.score AS score
    """
    edges = []
    for i in tqdm(range(0, len(pubs), BATCH_SIZE), desc=f"{label}"):
        batch = pubs[i : i + BATCH_SIZE]
        for r in session.run(query, batch=batch):
            edges.append((r["source"], r["target"], r["score"] if r["score"] is not None else 0.8))
    return edges


def fetch_citations(session, pubs):
    query = """
    UNWIND $batch AS source_pub
    MATCH (s:Patent {publication_number: source_pub})-[:CITES_PATENT]->(t:Patent)
    RETURN source_pub AS source, t.publication_number AS target
    """
    edges = []
    for i in tqdm(range(0, len(pubs), BATCH_SIZE), desc="cite"):
        batch = pubs[i : i + BATCH_SIZE]
        for r in session.run(query, batch=batch):
            edges.append((r["source"], r["target"]))
    return edges


def fetch_patent_details(session, pubs):
    """Fetch title, abstract, top assignee, IPC groups, filing year for a list of pubs."""
    query = """
    UNWIND $batch AS pub
    MATCH (p:Patent {publication_number: pub})
    OPTIONAL MATCH (p)-[:ASSIGNED_TO]->(a:Assignee)
    OPTIONAL MATCH (p)-[:CLASSIFIED_AS]->(g:IPC_Group)
    RETURN p.publication_number AS pub,
           p.title              AS title,
           p.abstract           AS abstract,
           p.filing_date        AS filing_date,
           p.country_code       AS country,
           collect(DISTINCT a.name)[0..3]   AS assignees,
           collect(DISTINCT g.code)[0..5]    AS ipc_groups
    """
    out = {}
    BATCH = 500
    for i in tqdm(range(0, len(pubs), BATCH), desc="details"):
        batch = pubs[i : i + BATCH]
        for r in session.run(query, batch=batch):
            out[r["pub"]] = {
                "title": r["title"],
                "abstract": r["abstract"],
                "filing_date": str(r["filing_date"]) if r["filing_date"] else None,
                "country": r["country"],
                "assignees": [a for a in (r["assignees"] or []) if a],
                "ipc_groups": list(r["ipc_groups"] or []),
            }
    return out


def main():
    # Load v3 cluster assignments
    print("Loading v3 cluster assignments...")
    with open(V3_DIR / "clusters.json") as f:
        clusters = json.load(f)
    with open(V3_DIR / "scope_metadata.json") as f:
        reps = json.load(f)

    pubs = [r["pub"] for r in reps]
    pub_to_idx = {p: i for i, p in enumerate(pubs)}
    n = len(pubs)
    print(f"  {n:,} family representatives loaded")

    # Connect and re-fetch all three edge types (same as v3 pipeline)
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        print("\nFetching CITES_PATENT edges...")
        cite_edges = fetch_citations(session, pubs)
        print(f"  {len(cite_edges):,} raw")

        print("Fetching SIMILAR_TITLE edges...")
        title_edges = fetch_edges_with_score(session, pubs, "SIMILAR_TITLE", "title")
        print(f"  {len(title_edges):,} raw")

        print("Fetching SIMILAR_ABSTRACT edges...")
        abs_edges = fetch_edges_with_score(session, pubs, "SIMILAR_ABSTRACT", "abstract")
        print(f"  {len(abs_edges):,} raw")

    # Build symmetric weighted edge dict, restricted to in-scope endpoints
    print("\nAggregating edge weights...")
    edge_weight = defaultdict(float)
    for src, tgt in cite_edges:
        if src in pub_to_idx and tgt in pub_to_idx and src != tgt:
            a, b = sorted((pub_to_idx[src], pub_to_idx[tgt]))
            edge_weight[(a, b)] += CITE_WEIGHT
    for src, tgt, score in title_edges:
        if src in pub_to_idx and tgt in pub_to_idx and src != tgt:
            a, b = sorted((pub_to_idx[src], pub_to_idx[tgt]))
            edge_weight[(a, b)] += score * TITLE_WEIGHT_FACTOR
    for src, tgt, score in abs_edges:
        if src in pub_to_idx and tgt in pub_to_idx and src != tgt:
            a, b = sorted((pub_to_idx[src], pub_to_idx[tgt]))
            edge_weight[(a, b)] += score * ABSTRACT_WEIGHT_FACTOR
    print(f"  {len(edge_weight):,} unique weighted edges")

    # For each level (coarse_03 mega, l2 sub), compute per-patent in-cluster degree
    results = {}
    pubs_to_fetch_details = set()

    for level in ["coarse_03", "l2"]:
        print(f"\n--- Level {level} ---")
        # Group patents by cluster
        cluster_to_idxs = defaultdict(list)
        for p, assign in clusters.items():
            if p in pub_to_idx:
                cluster_to_idxs[assign[level]].append(pub_to_idx[p])

        # Filter to clusters of meaningful size
        big_clusters = {cid: idxs for cid, idxs in cluster_to_idxs.items()
                        if len(idxs) >= MIN_CLUSTER_SIZE_TO_NAME}
        print(f"  {len(big_clusters)} clusters with >= {MIN_CLUSTER_SIZE_TO_NAME} patents")

        # Per-cluster: compute in-cluster weighted degree for each member
        level_results = []
        for cid, idxs in tqdm(sorted(big_clusters.items(),
                                      key=lambda x: -len(x[1])), desc=f"{level} clusters"):
            idx_set = set(idxs)
            in_degree = Counter()
            for (a, b), w in edge_weight.items():
                if a in idx_set and b in idx_set:
                    in_degree[a] += w
                    in_degree[b] += w
            top_indices = [idx for idx, _ in in_degree.most_common(TOP_K)]
            top_pubs = [pubs[i] for i in top_indices]
            top_degrees = [in_degree[i] for i in top_indices]
            pubs_to_fetch_details.update(top_pubs)
            level_results.append({
                "cluster_id": int(cid),
                "size": len(idxs),
                "top_central_pubs": list(zip(top_pubs, top_degrees)),
            })
        results[level] = level_results

    # Fetch full details for all top-central patents in one go
    print(f"\nFetching details for {len(pubs_to_fetch_details):,} central patents...")
    with driver.session() as session:
        details = fetch_patent_details(session, list(pubs_to_fetch_details))
    driver.close()

    # Merge details into results
    for level in results:
        for cluster in results[level]:
            enriched = []
            for pub, deg in cluster["top_central_pubs"]:
                d = details.get(pub, {})
                enriched.append({
                    "pub": pub,
                    "in_cluster_degree": round(deg, 1),
                    "title": d.get("title"),
                    "abstract": (d.get("abstract") or "")[:600],
                    "filing_date": d.get("filing_date"),
                    "country": d.get("country"),
                    "assignees": d.get("assignees", []),
                    "ipc_groups": d.get("ipc_groups", []),
                })
            cluster["top_central"] = enriched
            del cluster["top_central_pubs"]

    out_path = V3_DIR / "cluster_naming_data.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nNaming data written: {out_path}")
    print(f"  Mega clusters (coarse_03 ≥{MIN_CLUSTER_SIZE_TO_NAME}): {len(results['coarse_03'])}")
    print(f"  Sub clusters  (l2 ≥{MIN_CLUSTER_SIZE_TO_NAME}):        {len(results['l2'])}")


if __name__ == "__main__":
    main()
