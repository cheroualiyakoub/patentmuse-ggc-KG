"""
Experiment: fix the M0 mega over-merge (γ=0.3 puts 72% of the corpus in one cluster).

Two candidate fixes, both scored on the SAME held-out citation metric used in
validate_clustering.py (cluster on 80% of citations + all title/abstract, score the
held-out 20%), plus a 'balance' read (largest-cluster share, # of meaningful megas).

  A) gamma sweep at the mega level -- where does the blob break up?
  B) recursive (nested) Leiden -- keep the γ=0.3 partition, then re-cluster ONLY the
     M0 induced subgraph, replacing the blob with coherent sub-megas. M1..M5 untouched.

Read-only, uses the warm _validation_cache.pkl. No Neo4j writes.

Run: /home/yacoubcherouali/kg-builder/venv/bin/python fix_m0_experiment.py
"""
import pickle
import random
import time
from collections import Counter, defaultdict
from pathlib import Path

import igraph as ig
import leidenalg

SCRIPT_DIR = Path(__file__).resolve().parent
CACHE = SCRIPT_DIR / "_validation_cache.pkl"

CITE_WEIGHT = 3.0
TITLE_WEIGHT_FACTOR = 0.4
ABSTRACT_WEIGHT_FACTOR = 0.6
LEIDEN_SEED = 42
TEST_FRAC = 0.20
SPLIT_SEED = 1234
MEANINGFUL = 560  # ~1% of corpus -> counts as a "real" mega


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def undirected_pairs(directed, p2i, weighted):
    out = {}
    for e in directed:
        src, tgt = e[0], e[1]
        if src in p2i and tgt in p2i and src != tgt:
            a, b = sorted((p2i[src], p2i[tgt]))
            if weighted:
                out[(a, b)] = max(out.get((a, b), 0.0), e[2])
            else:
                out[(a, b)] = 1.0
    return out


def build_graph(n, cite_pairs, title_pairs, abs_pairs):
    ew = defaultdict(float)
    for ab in cite_pairs:
        ew[ab] += CITE_WEIGHT
    for ab, sc in title_pairs.items():
        ew[ab] += sc * TITLE_WEIGHT_FACTOR
    for ab, sc in abs_pairs.items():
        ew[ab] += sc * ABSTRACT_WEIGHT_FACTOR
    edges = list(ew.keys())
    g = ig.Graph(n=n, edges=edges, directed=False)
    g.es["weight"] = [ew[e] for e in edges]
    return g


def leiden(g, gamma):
    p = leidenalg.find_partition(
        g, leidenalg.RBConfigurationVertexPartition,
        resolution_parameter=gamma, weights="weight", seed=LEIDEN_SEED)
    return list(p.membership)


def metrics(mem, test, n):
    sizes = Counter(mem)
    largest = max(sizes.values())
    n_big = sum(1 for s in sizes.values() if s >= MEANINGFUL)
    null = sum(s * (s - 1) for s in sizes.values()) / (n * (n - 1))
    hits = sum(1 for (a, b) in test if mem[a] == mem[b])
    contain = hits / len(test)
    lift = contain / null if null else float("inf")
    return {"k": len(sizes), "largest": largest, "largest_pct": 100.0 * largest / n,
            "n_big": n_big, "contain": contain, "lift": lift}


def fmt(tag, m):
    return (f"{tag:26s} k={m['k']:>5,}  largest={m['largest']:>6,} "
            f"({m['largest_pct']:>4.1f}%)  megas≥1%={m['n_big']:>2}  "
            f"contain={m['contain']:.3f}  lift={m['lift']:>5.1f}")


def main():
    log("loading cache...")
    with open(CACHE, "rb") as f:
        data = pickle.load(f)
    pubs = data["pubs"]
    n = len(pubs)
    p2i = {p: i for i, p in enumerate(pubs)}

    cite_all = undirected_pairs(data["cite"], p2i, False)
    title_all = undirected_pairs(data["title"], p2i, True)
    abs_all = undirected_pairs(data["abstract"], p2i, True)

    keys = sorted(cite_all.keys())
    random.Random(SPLIT_SEED).shuffle(keys)
    n_test = int(len(keys) * TEST_FRAC)
    test = keys[:n_test]
    train = keys[n_test:]
    log(f"train={len(train):,} test={len(test):,}")

    g = build_graph(n, train, title_all, abs_all)

    print("\n" + "=" * 92)
    print("A) MEGA-LEVEL GAMMA SWEEP  (current mega = γ=0.3; want largest << 72%, few big megas)")
    print("=" * 92)
    baseline = None
    for gma in [0.3, 0.35, 0.4, 0.45, 0.5, 0.6]:
        mem = leiden(g, gma)
        m = metrics(mem, test, n)
        if gma == 0.3:
            baseline = m
        print(fmt(f"γ={gma}", m))

    print("\n" + "=" * 92)
    print("B) RECURSIVE (NESTED) LEIDEN  (split ONLY the M0 blob; M1..M5 untouched)")
    print("=" * 92)
    mem03 = leiden(g, 0.3)
    sizes03 = Counter(mem03)
    m0_id = max(sizes03, key=sizes03.get)
    m0_nodes = [i for i, c in enumerate(mem03) if c == m0_id]
    log(f"M0 = cluster {m0_id}: {len(m0_nodes):,} nodes ({100.0*len(m0_nodes)/n:.1f}%)")

    sub = g.induced_subgraph(m0_nodes)  # internal edges + weights preserved
    log(f"M0 subgraph: {sub.vcount():,} nodes, {sub.ecount():,} internal edges")

    for gsub in [0.5, 1.0, 1.5]:
        subpart = leidenalg.find_partition(
            sub, leidenalg.RBConfigurationVertexPartition,
            resolution_parameter=gsub, weights="weight", seed=LEIDEN_SEED)
        submem = list(subpart.membership)
        # build combined top-level membership
        combined = list(mem03)
        next_id = max(mem03) + 1
        local_map = {}
        for local_idx, node in enumerate(m0_nodes):
            sc = submem[local_idx]
            if sc not in local_map:
                local_map[sc] = next_id
                next_id += 1
            combined[node] = local_map[sc]
        m = metrics(combined, test, n)
        # sizes of M0's new pieces
        piece_sizes = sorted(Counter(submem).values(), reverse=True)
        big_pieces = [s for s in piece_sizes if s >= MEANINGFUL]
        print(fmt(f"M0 split @ γ_sub={gsub}", m))
        print(f"{'':26s}   -> M0 became {len(piece_sizes)} pieces; "
              f"{len(big_pieces)} are ≥1%: {big_pieces[:8]}")

    print("\n" + "=" * 92)
    print("READ: baseline γ=0.3 had largest=72.4%, 1 mega ≥1%. A good fix lowers")
    print("largest-share and raises # meaningful megas WITHOUT collapsing held-out lift.")
    print("=" * 92)


if __name__ == "__main__":
    main()
