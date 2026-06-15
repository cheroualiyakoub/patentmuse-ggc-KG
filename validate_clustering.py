"""
Quantitative validation for the H02P clustering: HELD-OUT CITATION PREDICTION.

Question answered: do our clusters group citing/cited patents together more than
chance -- and more than IPC does?

Anti-circularity: v3 builds clusters partly FROM citations (weight 3.0). So we
hold out 20% of in-scope citation pairs BEFORE clustering, re-cluster on the
remaining 80% (+ all title/abstract edges), and score the held-out 20%. IPC never
used citations, so it is a fair baseline on the same held-out edges.

Metric: lift = (held-out pairs whose endpoints share a cluster) / (expected under
a null model with the same cluster-size distribution). lift=1 means no better than
chance. Lift is size-aware, so a giant mega-cluster does NOT get rewarded for
trivially containing edges (its null co-membership is large too).

100% read-only on local Neo4j. No BigQuery. No writes. Caches fetched edges to a
local pickle so re-runs are instant.

Run (background + log):
    nohup /home/yacoubcherouali/kg-builder/venv/bin/python validate_clustering.py \
        > validate.log 2>&1 &
"""
import json
import os
import pickle
import random
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import igraph as ig
import leidenalg
from dotenv import load_dotenv
from neo4j import GraphDatabase

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "cluster_h02p_v3_out"
CACHE = SCRIPT_DIR / "_validation_cache.pkl"

load_dotenv(SCRIPT_DIR / ".env")
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PWD = os.getenv("NEO4J_PASSWORD")

SUBCLASS = "H02P"
COUNTRIES = ["US", "WO"]
BATCH_SIZE = 5000

# v3 weights (kept identical so we validate the SAME model that was built)
CITE_WEIGHT = 3.0
TITLE_WEIGHT_FACTOR = 0.4
ABSTRACT_WEIGHT_FACTOR = 0.6
RESOLUTIONS = {"coarse_03": 0.3, "l1": 0.5, "l2": 1.0, "l3": 2.0}
LEIDEN_SEED = 42

TEST_FRAC = 0.20
SPLIT_SEED = 1234


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ----------------------------------------------------------------------------
# Fetch (cached)
# ----------------------------------------------------------------------------
def fetch_all():
    if CACHE.exists():
        log(f"Loading cached graph data from {CACHE.name} ...")
        with open(CACHE, "rb") as f:
            return pickle.load(f)

    log("No cache. Fetching from Neo4j (one-time, ~3 min)...")
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    data = {}
    with driver.session() as s:
        t0 = time.time()
        rep_q = """
        MATCH (p:Patent)-[:CLASSIFIED_AS]->(:IPC_Group)-[:PART_OF]->(:IPC_Subclass {code:$subclass})
        WHERE p.country_code IN $countries
        MATCH (p)-[:BELONGS_TO_FAMILY]->(f:Family)
        WITH p, f
        ORDER BY p.filing_date ASC, p.publication_number ASC
        WITH f, collect(p)[0] AS rep
        RETURN collect(rep.publication_number) AS pubs
        """
        pubs = s.run(rep_q, subclass=SUBCLASS, countries=COUNTRIES).single()["pubs"]
        data["pubs"] = pubs
        log(f"  reps: {len(pubs):,} ({time.time()-t0:.1f}s)")

        def fetch_edges(edge_type, arrow, has_score):
            score_proj = ", r.score AS score" if has_score else ""
            q = f"""
            UNWIND $batch AS sp
            MATCH (s:Patent {{publication_number: sp}}){arrow}(t:Patent)
            RETURN sp AS source, t.publication_number AS target{score_proj}
            """
            edges = []
            t0 = time.time()
            for i in range(0, len(pubs), BATCH_SIZE):
                batch = pubs[i:i+BATCH_SIZE]
                for r in s.run(q, batch=batch):
                    if has_score:
                        sc = r["score"] if r["score"] is not None else 0.8
                        edges.append((r["source"], r["target"], sc))
                    else:
                        edges.append((r["source"], r["target"]))
            log(f"  {edge_type}: {len(edges):,} directed ({time.time()-t0:.1f}s)")
            return edges

        data["cite"] = fetch_edges("CITES_PATENT", "-[r:CITES_PATENT]->", False)
        data["title"] = fetch_edges("SIMILAR_TITLE", "-[r:SIMILAR_TITLE]-", True)
        data["abstract"] = fetch_edges("SIMILAR_ABSTRACT", "-[r:SIMILAR_ABSTRACT]-", True)

        # primary IPC group per rep (first listed) for the IPC baseline partition
        ipc_q = """
        UNWIND $batch AS sp
        MATCH (p:Patent {publication_number: sp})-[:CLASSIFIED_AS]->(g:IPC_Group)
        RETURN sp AS pub, collect(g.code) AS groups
        """
        ipc = {}
        t0 = time.time()
        for i in range(0, len(pubs), BATCH_SIZE):
            batch = pubs[i:i+BATCH_SIZE]
            for r in s.run(ipc_q, batch=batch):
                ipc[r["pub"]] = r["groups"]
        data["ipc"] = ipc
        log(f"  IPC groups for {len(ipc):,} reps ({time.time()-t0:.1f}s)")
    driver.close()

    with open(CACHE, "wb") as f:
        pickle.dump(data, f)
    log(f"  cached -> {CACHE.name}")
    return data


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def undirected_pairs(directed_edges, pub_to_idx, weighted):
    """Collapse directed edges to unique undirected in-scope idx-pairs."""
    out = {}
    for e in directed_edges:
        src, tgt = e[0], e[1]
        if src in pub_to_idx and tgt in pub_to_idx and src != tgt:
            a, b = sorted((pub_to_idx[src], pub_to_idx[tgt]))
            if weighted:
                out[(a, b)] = max(out.get((a, b), 0.0), e[2])
            else:
                out[(a, b)] = 1.0
    return out


def build_and_cluster(n, cite_pairs, title_pairs, abs_pairs, gamma):
    ew = defaultdict(float)
    for (a, b) in cite_pairs:
        ew[(a, b)] += CITE_WEIGHT
    for (a, b), sc in title_pairs.items():
        ew[(a, b)] += sc * TITLE_WEIGHT_FACTOR
    for (a, b), sc in abs_pairs.items():
        ew[(a, b)] += sc * ABSTRACT_WEIGHT_FACTOR
    edges = list(ew.keys())
    g = ig.Graph(n=n, edges=edges, directed=False)
    g.es["weight"] = [ew[e] for e in edges]
    part = leidenalg.find_partition(
        g, leidenalg.RBConfigurationVertexPartition,
        resolution_parameter=gamma, weights="weight", seed=LEIDEN_SEED,
    )
    return list(part.membership)


def null_comembership(membership, n):
    """Expected P(two random distinct nodes share a cluster) given sizes."""
    sizes = Counter(membership)
    same = sum(s * (s - 1) for s in sizes.values())
    total = n * (n - 1)
    return same / total if total else 0.0


def score_partition(membership, test_pairs, n):
    """Fraction of held-out pairs whose endpoints share a cluster, and lift."""
    if not test_pairs:
        return 0.0, 0.0, 0.0, 0
    hits = sum(1 for (a, b) in test_pairs if membership[a] == membership[b])
    contain = hits / len(test_pairs)
    null = null_comembership(membership, n)
    lift = contain / null if null > 0 else float("inf")
    n_clusters = len(set(membership))
    return contain, null, lift, n_clusters


def ipc_partition(pubs, ipc_map, level):
    """Assign each rep one cluster id from its PRIMARY (first) IPC group.
    level='group' -> full code (H02P21/22); level='main' -> truncated (H02P21)."""
    label_to_id = {}
    membership = []
    for pub in pubs:
        groups = ipc_map.get(pub) or []
        code = groups[0] if groups else "UNKNOWN"
        if level == "main":
            code = code.split("/")[0]
        if code not in label_to_id:
            label_to_id[code] = len(label_to_id)
        membership.append(label_to_id[code])
    return membership


def random_partition(n, k, seed):
    rng = random.Random(seed)
    return [rng.randrange(k) for _ in range(n)]


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    data = fetch_all()
    pubs = data["pubs"]
    n = len(pubs)
    pub_to_idx = {p: i for i, p in enumerate(pubs)}

    cite_all = undirected_pairs(data["cite"], pub_to_idx, weighted=False)
    title_all = undirected_pairs(data["title"], pub_to_idx, weighted=True)
    abs_all = undirected_pairs(data["abstract"], pub_to_idx, weighted=True)
    log(f"unique in-scope pairs -> cite={len(cite_all):,} title={len(title_all):,} abstract={len(abs_all):,}")

    # 80/20 split of citation pairs
    cite_keys = sorted(cite_all.keys())
    rng = random.Random(SPLIT_SEED)
    rng.shuffle(cite_keys)
    n_test = int(len(cite_keys) * TEST_FRAC)
    test_pairs = cite_keys[:n_test]
    train_pairs = cite_keys[n_test:]
    log(f"citation split -> train={len(train_pairs):,}  held-out test={len(test_pairs):,}")

    results = []

    # Leiden methods (trained WITHOUT the held-out citations)
    for level, gamma in RESOLUTIONS.items():
        t0 = time.time()
        mem = build_and_cluster(n, train_pairs, title_all, abs_all, gamma)
        contain, null, lift, k = score_partition(mem, test_pairs, n)
        log(f"Leiden {level} (γ={gamma}): k={k:,} contain={contain:.3f} null={null:.5f} "
            f"LIFT={lift:.1f}  ({time.time()-t0:.1f}s)")
        results.append({"method": f"Leiden γ={gamma} ({level})", "k": k,
                        "mean_size": round(n / k, 1), "containment": round(contain, 4),
                        "null": round(null, 6), "lift": round(lift, 2)})

    # IPC baselines (never saw citations -> fair)
    for level, name in [("main", "IPC main-group (H02Pxx)"), ("group", "IPC full-group")]:
        mem = ipc_partition(pubs, data["ipc"], level)
        contain, null, lift, k = score_partition(mem, test_pairs, n)
        log(f"{name}: k={k:,} contain={contain:.3f} null={null:.5f} LIFT={lift:.1f}")
        results.append({"method": name, "k": k, "mean_size": round(n / k, 1),
                        "containment": round(contain, 4), "null": round(null, 6),
                        "lift": round(lift, 2)})

    # Random baselines matched to Leiden cluster counts (sanity: lift ~ 1)
    for level, gamma in [("coarse_03", 0.3), ("l2", 1.0)]:
        k = len(set(build_and_cluster(n, train_pairs, title_all, abs_all, gamma)))
        mem = random_partition(n, k, SPLIT_SEED)
        contain, null, lift, _ = score_partition(mem, test_pairs, n)
        log(f"Random (k={k:,}, matched to γ={gamma}): contain={contain:.4f} LIFT={lift:.2f}")
        results.append({"method": f"Random (k≈{k}, matched γ={gamma})", "k": k,
                        "mean_size": round(n / k, 1), "containment": round(contain, 4),
                        "null": round(null, 6), "lift": round(lift, 2)})

    # Table
    log("\n=== HELD-OUT CITATION PREDICTION (higher lift = better) ===")
    hdr = f"{'method':38s} {'#clust':>7s} {'mean':>7s} {'contain':>8s} {'lift':>7s}"
    print(hdr, flush=True)
    print("-" * len(hdr), flush=True)
    for r in results:
        print(f"{r['method']:38s} {r['k']:>7,} {r['mean_size']:>7} "
              f"{r['containment']:>8.3f} {r['lift']:>7.1f}", flush=True)

    out = {"scope": {"subclass": SUBCLASS, "countries": COUNTRIES, "n_reps": n},
           "test_frac": TEST_FRAC, "n_test_edges": len(test_pairs),
           "split_seed": SPLIT_SEED, "results": results}
    with open(OUT_DIR / "validation_results.json", "w") as f:
        json.dump(out, f, indent=2)
    log(f"\nSaved -> {OUT_DIR/'validation_results.json'}")


if __name__ == "__main__":
    main()
