"""
v3 clustering: weighted multi-view Leiden combining
  - CITES_PATENT       (strong, sparse)
  - SIMILAR_TITLE      (moderate, ~100% coverage)
  - SIMILAR_ABSTRACT   (strong semantic, ~91% coverage)

Edge weighting calibrated so each signal contributes ~comparable total weight per
node, with citations slightly favored per-edge for their reliability:
  per-node weight ≈ 5 cites × 3.0      = 15
                  + 30 title × 0.4×0.85 ≈ 10
                  + 29 abs   × 0.6×0.85 ≈ 15

Run:
    /home/yacoubcherouali/kg-builder/venv/bin/python cluster_h02p_v3.py
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

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "cluster_h02p_v3_out"
OUT_DIR.mkdir(exist_ok=True)

load_dotenv(SCRIPT_DIR / ".env")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

SUBCLASS = "H02P"
COUNTRIES = ["US", "WO"]
BATCH_SIZE = 5000
RESOLUTIONS = {
    "coarse_03": 0.3,
    "l1": 0.5,
    "l2": 1.0,
    "l3": 2.0,
}
LEIDEN_SEED = 42

CITE_WEIGHT = 3.0
TITLE_WEIGHT_FACTOR = 0.4
ABSTRACT_WEIGHT_FACTOR = 0.6


def fetch_representatives(session):
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
    print(f"[1/6] Family representatives for {SUBCLASS} {COUNTRIES}...")
    t0 = time.time()
    rows = [dict(r) for r in session.run(query, subclass=SUBCLASS, countries=COUNTRIES)]
    print(f"      {len(rows):,} representatives in {time.time()-t0:.1f}s")
    return rows


def _fetch_edges(session, pubs, edge_type, label, has_score=True):
    score_proj = ", r.score AS score" if has_score else ""
    query = f"""
    UNWIND $batch AS source_pub
    MATCH (s:Patent {{publication_number: source_pub}})-[r:{edge_type}]-(t:Patent)
    RETURN source_pub AS source, t.publication_number AS target{score_proj}
    """
    print(f"      Fetching {label}...")
    edges = []
    t0 = time.time()
    for i in tqdm(range(0, len(pubs), BATCH_SIZE), desc=f"{label} batches"):
        batch = pubs[i : i + BATCH_SIZE]
        for r in session.run(query, batch=batch):
            if has_score:
                edges.append((r["source"], r["target"], r["score"] if r["score"] is not None else 0.8))
            else:
                edges.append((r["source"], r["target"]))
    print(f"      {len(edges):,} {label} edges in {time.time()-t0:.1f}s")
    return edges


def fetch_citations(session, pubs):
    print("[2/6] Fetching CITES_PATENT edges...")
    query = """
    UNWIND $batch AS source_pub
    MATCH (s:Patent {publication_number: source_pub})-[:CITES_PATENT]->(t:Patent)
    RETURN source_pub AS source, t.publication_number AS target
    """
    edges = []
    t0 = time.time()
    for i in tqdm(range(0, len(pubs), BATCH_SIZE), desc="cite batches"):
        batch = pubs[i : i + BATCH_SIZE]
        for r in session.run(query, batch=batch):
            edges.append((r["source"], r["target"]))
    print(f"      {len(edges):,} CITES_PATENT edges in {time.time()-t0:.1f}s")
    return edges


def fetch_similar_title(session, pubs):
    print("[3/6] Fetching SIMILAR_TITLE edges...")
    return _fetch_edges(session, pubs, "SIMILAR_TITLE", "SIMILAR_TITLE")


def fetch_similar_abstract(session, pubs):
    print("[4/6] Fetching SIMILAR_ABSTRACT edges...")
    return _fetch_edges(session, pubs, "SIMILAR_ABSTRACT", "SIMILAR_ABSTRACT")


def fetch_ipc_per_rep(session, pubs):
    print("[5/6] Fetching IPC groups...")
    query = """
    UNWIND $batch AS pub
    MATCH (p:Patent {publication_number: pub})-[:CLASSIFIED_AS]->(g:IPC_Group)
    RETURN pub, collect(DISTINCT g.code) AS ipc_groups
    """
    out = {}
    t0 = time.time()
    for i in tqdm(range(0, len(pubs), BATCH_SIZE), desc="ipc batches"):
        batch = pubs[i : i + BATCH_SIZE]
        for r in session.run(query, batch=batch):
            out[r["pub"]] = r["ipc_groups"]
    print(f"      IPC for {len(out):,} reps in {time.time()-t0:.1f}s")
    return out


def build_graph(reps, cite_edges, title_edges, abstract_edges):
    pub_to_idx = {p["pub"]: i for i, p in enumerate(reps)}

    edge_weight = defaultdict(float)
    n_cite = n_title = n_abs = 0

    for src, tgt in cite_edges:
        if src in pub_to_idx and tgt in pub_to_idx and src != tgt:
            a, b = sorted((pub_to_idx[src], pub_to_idx[tgt]))
            edge_weight[(a, b)] += CITE_WEIGHT
            n_cite += 1

    for src, tgt, score in title_edges:
        if src in pub_to_idx and tgt in pub_to_idx and src != tgt:
            a, b = sorted((pub_to_idx[src], pub_to_idx[tgt]))
            edge_weight[(a, b)] += score * TITLE_WEIGHT_FACTOR
            n_title += 1

    for src, tgt, score in abstract_edges:
        if src in pub_to_idx and tgt in pub_to_idx and src != tgt:
            a, b = sorted((pub_to_idx[src], pub_to_idx[tgt]))
            edge_weight[(a, b)] += score * ABSTRACT_WEIGHT_FACTOR
            n_abs += 1

    print(f"      Citation edges in-scope:        {n_cite:,}")
    print(f"      SIMILAR_TITLE edges in-scope:   {n_title:,}")
    print(f"      SIMILAR_ABSTRACT in-scope:      {n_abs:,}")
    print(f"      Total unique weighted edges:    {len(edge_weight):,}")

    edges = list(edge_weight.keys())
    weights = [edge_weight[e] for e in edges]
    g = ig.Graph(n=len(reps), edges=edges, directed=False)
    g.es["weight"] = weights
    g.vs["pub"] = [p["pub"] for p in reps]
    return g, pub_to_idx


def run_leiden(g, resolutions):
    print("[6/6] Weighted Leiden at multiple resolutions...")
    out = {}
    for level, gamma in resolutions.items():
        t0 = time.time()
        part = leidenalg.find_partition(
            g,
            leidenalg.RBConfigurationVertexPartition,
            resolution_parameter=gamma,
            weights="weight",
            seed=LEIDEN_SEED,
        )
        membership = list(part.membership)
        non_singleton = sum(1 for _, c in Counter(membership).items() if c > 1)
        print(f"      {level} (γ={gamma}): {len(set(membership)):,} clusters "
              f"({non_singleton:,} non-singleton), Q={part.modularity:.4f}, "
              f"{time.time()-t0:.1f}s")
        out[level] = membership
    return out


def summarize_clusters(reps, ipc_map, memberships, level, top_n=10, sample_titles=10):
    cluster_to_idxs = defaultdict(list)
    for idx, cid in enumerate(memberships[level]):
        cluster_to_idxs[cid].append(idx)

    summaries = []
    for cid, idxs in sorted(cluster_to_idxs.items(), key=lambda x: -len(x[1])):
        if len(idxs) < 2:
            continue
        assignee_ctr = Counter()
        ipc_ctr = Counter()
        country_ctr = Counter()
        for idx in idxs:
            for a in reps[idx]["assignees"] or []:
                if a:
                    assignee_ctr[a] += 1
            for gcode in ipc_map.get(reps[idx]["pub"], []):
                ipc_ctr[gcode] += 1
            country_ctr[reps[idx]["country"]] += 1
        summaries.append({
            "cluster_id": int(cid),
            "size": len(idxs),
            "top_assignees": assignee_ctr.most_common(top_n),
            "top_ipc_groups": ipc_ctr.most_common(top_n),
            "country_mix": country_ctr.most_common(),
            "sample_titles": [reps[i]["title"] for i in idxs[:sample_titles]],
        })
    return summaries


def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        reps = fetch_representatives(session)
        pubs = [r["pub"] for r in reps]
        cite_edges = fetch_citations(session, pubs)
        title_edges = fetch_similar_title(session, pubs)
        abstract_edges = fetch_similar_abstract(session, pubs)
        ipc_map = fetch_ipc_per_rep(session, pubs)
    driver.close()

    print("\n[graph] Building weighted 3-view igraph...")
    g, pub_to_idx = build_graph(reps, cite_edges, title_edges, abstract_edges)
    components = g.connected_components()
    sizes = sorted((len(c) for c in components), reverse=True)
    print(f"      {len(components):,} connected components | "
          f"largest: {sizes[0]:,} | "
          f"isolated (size 1): {sum(1 for s in sizes if s == 1):,}")

    memberships = run_leiden(g, RESOLUTIONS)

    print("\n[save] Writing outputs...")
    with open(OUT_DIR / "scope_metadata.json", "w") as f:
        json.dump(reps, f, default=str)

    cluster_assignments = {}
    for idx, p in enumerate(reps):
        cluster_assignments[p["pub"]] = {
            level: int(memberships[level][idx]) for level in RESOLUTIONS
        }
    with open(OUT_DIR / "clusters.json", "w") as f:
        json.dump(cluster_assignments, f)

    summary_counts = {}
    for level in RESOLUTIONS:
        sums = summarize_clusters(reps, ipc_map, memberships, level=level)
        with open(OUT_DIR / f"cluster_summary_{level}.json", "w") as f:
            json.dump(sums, f, default=str, indent=2)
        summary_counts[level] = len(sums)

    print(f"\nOutputs in {OUT_DIR}/")
    for level, n in summary_counts.items():
        print(f"  - cluster_summary_{level}.json ({n:,} non-singleton clusters at γ={RESOLUTIONS[level]})")
    print("\nDone. No mutations to Neo4j.")


if __name__ == "__main__":
    main()
