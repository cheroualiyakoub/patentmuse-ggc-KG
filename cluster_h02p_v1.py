"""
Cluster H02P patents (US+WO family representatives) via Leiden community detection
on the direct citation graph. v1: citation-only signal. Writes results to local JSON;
does NOT mutate Neo4j.

Run:
    /home/yacoubcherouali/kg-builder/venv/bin/python cluster_h02p_v1.py
"""

import json
import os
import time
from collections import Counter, defaultdict
from pathlib import Path

import igraph as ig
import leidenalg
from dotenv import load_dotenv
from neo4j import GraphDatabase
from tqdm import tqdm

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "cluster_h02p_v1_out"
OUT_DIR.mkdir(exist_ok=True)

load_dotenv(SCRIPT_DIR / ".env")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

SUBCLASS = "H02P"
COUNTRIES = ["US", "WO"]
BATCH_SIZE = 5000   # for UNWIND batched queries
RESOLUTIONS = {
    "coarse_01": 0.1,
    "coarse_03": 0.3,
    "l1": 0.5,
    "l2": 1.0,
    "l3": 2.0,
}
LEIDEN_SEED = 42


# ------------------------------------------------------------------------------
# Neo4j helpers
# ------------------------------------------------------------------------------
def fetch_representatives(session):
    """For each Family with >=1 H02P member in {US, WO}, return the earliest-filing
    such member as the family representative, with assignees + filing date."""
    query = """
    MATCH (p:Patent)-[:CLASSIFIED_AS]->(:IPC_Group)-[:PART_OF]->(:IPC_Subclass {code:$subclass})
    WHERE p.country_code IN $countries
    MATCH (p)-[:BELONGS_TO_FAMILY]->(f:Family)
    WITH p, f
    ORDER BY p.filing_date ASC, p.publication_number ASC
    WITH f, collect(p)[0] AS rep
    OPTIONAL MATCH (rep)-[:ASSIGNED_TO]->(a:Assignee)
    WITH f, rep, collect(DISTINCT a.name) AS assignees
    RETURN f.family_id        AS family_id,
           rep.publication_number AS pub,
           rep.title           AS title,
           rep.country_code    AS country,
           rep.filing_date     AS filing_date,
           assignees
    """
    print(f"[1/4] Fetching family representatives for {SUBCLASS} {COUNTRIES}...")
    t0 = time.time()
    result = session.run(query, subclass=SUBCLASS, countries=COUNTRIES)
    rows = [dict(r) for r in result]
    print(f"      {len(rows):,} representatives in {time.time()-t0:.1f}s")
    return rows


def fetch_citations(session, pubs):
    """All CITES_PATENT edges from rep -> any patent. We filter target ∈ reps in Python."""
    query = """
    UNWIND $batch AS source_pub
    MATCH (s:Patent {publication_number: source_pub})-[:CITES_PATENT]->(t:Patent)
    RETURN source_pub AS source, t.publication_number AS target
    """
    print(f"[2/4] Fetching outgoing citations for {len(pubs):,} reps (batches of {BATCH_SIZE})...")
    edges = []
    t0 = time.time()
    for i in tqdm(range(0, len(pubs), BATCH_SIZE), desc="cite batches"):
        batch = pubs[i : i + BATCH_SIZE]
        for r in session.run(query, batch=batch):
            edges.append((r["source"], r["target"]))
    print(f"      {len(edges):,} raw outgoing edges in {time.time()-t0:.1f}s")
    return edges


def fetch_ipc_per_rep(session, pubs):
    """IPC_Group codes per rep (for cluster diagnostics)."""
    query = """
    UNWIND $batch AS pub
    MATCH (p:Patent {publication_number: pub})-[:CLASSIFIED_AS]->(g:IPC_Group)
    RETURN pub, collect(DISTINCT g.code) AS ipc_groups
    """
    print(f"[3/4] Fetching IPC groups for {len(pubs):,} reps...")
    out = {}
    t0 = time.time()
    for i in tqdm(range(0, len(pubs), BATCH_SIZE), desc="ipc batches"):
        batch = pubs[i : i + BATCH_SIZE]
        for r in session.run(query, batch=batch):
            out[r["pub"]] = r["ipc_groups"]
    print(f"      IPC for {len(out):,} reps in {time.time()-t0:.1f}s")
    return out


# ------------------------------------------------------------------------------
# Graph construction & clustering
# ------------------------------------------------------------------------------
def build_graph(reps, edges_raw):
    """Build undirected igraph. Nodes = reps. Edges = in-scope citations (deduped)."""
    pub_to_idx = {p["pub"]: i for i, p in enumerate(reps)}
    in_scope_edges = set()
    for src, tgt in edges_raw:
        if src in pub_to_idx and tgt in pub_to_idx and src != tgt:
            a, b = sorted((pub_to_idx[src], pub_to_idx[tgt]))
            in_scope_edges.add((a, b))
    print(f"      {len(in_scope_edges):,} in-scope citation edges (deduped, undirected)")

    g = ig.Graph(n=len(reps), edges=list(in_scope_edges), directed=False)
    g.vs["pub"] = [p["pub"] for p in reps]
    return g, pub_to_idx


def run_leiden(g, resolutions):
    """Run Leiden at each resolution. Returns {level_name: list_of_cluster_ids}."""
    print("[4/4] Running Leiden at multiple resolutions...")
    out = {}
    for level, gamma in resolutions.items():
        t0 = time.time()
        part = leidenalg.find_partition(
            g,
            leidenalg.RBConfigurationVertexPartition,
            resolution_parameter=gamma,
            seed=LEIDEN_SEED,
        )
        membership = list(part.membership)
        non_singleton = sum(1 for _, c in Counter(membership).items() if c > 1)
        print(
            f"      {level} (γ={gamma}): {len(set(membership)):,} clusters "
            f"({non_singleton:,} non-singleton), Q={part.modularity:.4f}, "
            f"{time.time()-t0:.1f}s"
        )
        out[level] = membership
    return out


# ------------------------------------------------------------------------------
# Cluster summaries
# ------------------------------------------------------------------------------
def summarize_clusters(reps, ipc_map, memberships, level="l2", top_n=10, sample_titles=10):
    """For the chosen level, produce per-cluster summaries:
    size, top assignees, top IPC groups, sample titles."""
    cluster_to_idxs = defaultdict(list)
    for idx, cid in enumerate(memberships[level]):
        cluster_to_idxs[cid].append(idx)

    summaries = []
    for cid, idxs in sorted(cluster_to_idxs.items(), key=lambda x: -len(x[1])):
        if len(idxs) < 2:
            continue  # skip singletons in the summary
        assignee_ctr = Counter()
        ipc_ctr = Counter()
        country_ctr = Counter()
        for idx in idxs:
            for a in reps[idx]["assignees"] or []:
                if a:
                    assignee_ctr[a] += 1
            for g in ipc_map.get(reps[idx]["pub"], []):
                ipc_ctr[g] += 1
            country_ctr[reps[idx]["country"]] += 1
        sample_idxs = idxs[:sample_titles]
        summaries.append(
            {
                "cluster_id": int(cid),
                "size": len(idxs),
                "top_assignees": assignee_ctr.most_common(top_n),
                "top_ipc_groups": ipc_ctr.most_common(top_n),
                "country_mix": country_ctr.most_common(),
                "sample_titles": [reps[i]["title"] for i in sample_idxs],
            }
        )
    return summaries


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        reps = fetch_representatives(session)
        pubs = [r["pub"] for r in reps]

        edges_raw = fetch_citations(session, pubs)
        ipc_map = fetch_ipc_per_rep(session, pubs)

    driver.close()

    print("\n[graph] Building igraph...")
    g, pub_to_idx = build_graph(reps, edges_raw)

    # Connectivity diagnostic
    components = g.connected_components()
    sizes = sorted((len(c) for c in components), reverse=True)
    print(
        f"      {len(components):,} connected components | "
        f"largest: {sizes[0]:,} | "
        f"isolated (size 1): {sum(1 for s in sizes if s == 1):,}"
    )

    memberships = run_leiden(g, RESOLUTIONS)

    print("\n[save] Writing outputs...")
    # 1. scope_metadata.json
    with open(OUT_DIR / "scope_metadata.json", "w") as f:
        json.dump(reps, f, default=str)

    # 2. clusters.json — pub_number → cluster IDs at each level
    cluster_assignments = {}
    for idx, p in enumerate(reps):
        cluster_assignments[p["pub"]] = {
            level: int(memberships[level][idx]) for level in RESOLUTIONS
        }
    with open(OUT_DIR / "clusters.json", "w") as f:
        json.dump(cluster_assignments, f)

    # 3. cluster_summary_<level>.json — one summary per resolution level
    summary_counts = {}
    for level in RESOLUTIONS:
        sums = summarize_clusters(reps, ipc_map, memberships, level=level)
        with open(OUT_DIR / f"cluster_summary_{level}.json", "w") as f:
            json.dump(sums, f, default=str, indent=2)
        summary_counts[level] = len(sums)

    # 4. cluster_sizes.json — distribution at all 3 levels
    size_dist = {
        level: dict(Counter(Counter(memberships[level]).values()))
        for level in RESOLUTIONS
    }
    with open(OUT_DIR / "cluster_sizes.json", "w") as f:
        json.dump(size_dist, f, default=str, indent=2)

    print(f"\nOutputs in {OUT_DIR}/")
    print(f"  - scope_metadata.json   ({len(reps):,} reps)")
    print(f"  - clusters.json         ({len(reps):,} assignments)")
    for level, n in summary_counts.items():
        print(f"  - cluster_summary_{level}.json ({n:,} non-singleton clusters at γ={RESOLUTIONS[level]})")
    print(f"  - cluster_sizes.json    (size distribution per level)")
    print("\nDone. No mutations to Neo4j.")


if __name__ == "__main__":
    main()
