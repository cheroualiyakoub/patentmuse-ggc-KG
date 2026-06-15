"""
v4 clustering: fixes the M0 mega over-merge via RECURSIVE (nested) Leiden.

v3's flat γ=0.3 mega level put ~72% of the corpus in one cluster (M0). v4 keeps the
γ=0.3 partition but recursively re-clusters ANY mega larger than SPLIT_THRESHOLD of
the corpus at γ=1.0, replacing each oversized blob with coherent sub-megas. This
yields a balanced top level AND a genuine 2-level hierarchy (mega_fixed -> the γ=1.0
'l2' sub level), instead of v3's majority-vote parenting.

Produces, on the FULL edge graph (all citations + title + abstract):
  - level 'mega_fixed' : the new balanced top taxonomy
  - levels l1/l2/l3    : same global Leiden levels as v3 (for continuity)
And separately VALIDATES the recursive procedure on a held-out citation split,
confirming held-out containment stays well above IPC.

Read-only on the warm cache. Writes JSON to cluster_h02p_v4_out/. No Neo4j writes.

Run: /home/yacoubcherouali/kg-builder/venv/bin/python cluster_h02p_v4.py
"""
import json
import pickle
import random
import time
from collections import Counter, defaultdict
from pathlib import Path

import igraph as ig
import leidenalg

SCRIPT_DIR = Path(__file__).resolve().parent
CACHE = SCRIPT_DIR / "_validation_cache.pkl"
OUT_DIR = SCRIPT_DIR / "cluster_h02p_v4_out"
OUT_DIR.mkdir(exist_ok=True)

CITE_WEIGHT = 3.0
TITLE_WEIGHT_FACTOR = 0.4
ABSTRACT_WEIGHT_FACTOR = 0.6
LEIDEN_SEED = 42
MEGA_GAMMA = 0.3
SPLIT_GAMMA = 1.0
SPLIT_THRESHOLD_FRAC = 0.15   # re-cluster any mega bigger than 15% of the corpus
SUB_LEVELS = {"l1": 0.5, "l2": 1.0, "l3": 2.0}
TEST_FRAC = 0.20
SPLIT_SEED = 1234
MEANINGFUL = 560


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
    return list(leidenalg.find_partition(
        g, leidenalg.RBConfigurationVertexPartition,
        resolution_parameter=gamma, weights="weight", seed=LEIDEN_SEED).membership)


def recursive_megas(g, n):
    """γ=0.3 megas, then split any mega > threshold at γ=1.0. Returns membership list
    and a parent map {final_id: original_mega_id} for hierarchy provenance."""
    base = leiden(g, MEGA_GAMMA)
    threshold = int(SPLIT_THRESHOLD_FRAC * n)
    sizes = Counter(base)
    final = list(base)
    parent = {cid: cid for cid in sizes}        # final_id -> base mega id
    next_id = max(base) + 1
    n_split = 0
    for cid, size in sizes.items():
        if size <= threshold:
            continue
        nodes = [i for i, c in enumerate(base) if c == cid]
        sub = g.induced_subgraph(nodes)
        submem = list(leidenalg.find_partition(
            sub, leidenalg.RBConfigurationVertexPartition,
            resolution_parameter=SPLIT_GAMMA, weights="weight", seed=LEIDEN_SEED).membership)
        local_map = {}
        for local_idx, node in enumerate(nodes):
            sc = submem[local_idx]
            if sc == 0:
                final[node] = cid          # keep one piece under the original id
            else:
                if sc not in local_map:
                    local_map[sc] = next_id
                    parent[next_id] = cid
                    next_id += 1
                final[node] = local_map[sc]
        n_split += 1
        log(f"  split mega {cid} ({size:,}) -> {1+len(local_map)} pieces")
    log(f"  recursively split {n_split} oversized mega(s)")
    return final, parent


def dist(mem, n):
    sizes = Counter(mem)
    return max(sizes.values()), len(sizes), sum(1 for s in sizes.values() if s >= MEANINGFUL)


def validate(g_train, test, n, label):
    mem, _ = recursive_megas(g_train, n)
    sizes = Counter(mem)
    null = sum(s * (s - 1) for s in sizes.values()) / (n * (n - 1))
    hits = sum(1 for (a, b) in test if mem[a] == mem[b])
    contain = hits / len(test)
    log(f"  [{label}] held-out containment={contain:.3f}  lift={contain/null:.1f}  "
        f"(IPC main-group baseline = 0.256)")
    return contain


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

    # ---- FINAL taxonomy on the FULL graph (all edges) ----
    log("building full-graph taxonomy...")
    g_full = build_graph(n, list(cite_all.keys()), title_all, abs_all)

    base = leiden(g_full, MEGA_GAMMA)
    bl, bk, bbig = dist(base, n)
    log(f"v3-style mega (γ=0.3): largest={bl:,} ({100.0*bl/n:.1f}%)  megas≥1%={bbig}")

    mega_fixed, parent = recursive_megas(g_full, n)
    fl, fk, fbig = dist(mega_fixed, n)
    log(f"v4 mega_fixed: largest={fl:,} ({100.0*fl/n:.1f}%)  clusters={fk}  megas≥1%={fbig}")

    memberships = {"mega_fixed": mega_fixed}
    for lvl, gma in SUB_LEVELS.items():
        memberships[lvl] = leiden(g_full, gma)

    # ---- write outputs ----
    assignments = {}
    for idx, pub in enumerate(pubs):
        assignments[pub] = {lvl: int(memberships[lvl][idx]) for lvl in memberships}
    with open(OUT_DIR / "clusters.json", "w") as f:
        json.dump(assignments, f)
    with open(OUT_DIR / "mega_fixed_parent.json", "w") as f:
        json.dump({str(k): int(v) for k, v in parent.items()}, f)

    # top mega_fixed clusters with their dominant l2 sub (for quick labeling)
    l2 = memberships["l2"]
    fixed_sizes = Counter(mega_fixed)
    report = []
    for cid, size in sorted(fixed_sizes.items(), key=lambda x: -x[1]):
        if size < MEANINGFUL:
            continue
        members = [i for i, c in enumerate(mega_fixed) if c == cid]
        dom_l2 = Counter(l2[i] for i in members).most_common(1)[0]
        report.append({"cluster_id": int(cid), "size": size,
                       "parent_mega": int(parent[cid]),
                       "dominant_l2": dom_l2[0], "dominant_l2_share": round(dom_l2[1]/size, 2)})
    with open(OUT_DIR / "mega_fixed_report.json", "w") as f:
        json.dump(report, f, indent=2)
    log(f"v4 has {len(report)} meaningful megas (≥1%). Top 12 sizes: "
        f"{[r['size'] for r in report[:12]]}")

    # ---- validate the recursive procedure on a held-out split ----
    log("validating on held-out citations...")
    keys = sorted(cite_all.keys())
    random.Random(SPLIT_SEED).shuffle(keys)
    n_test = int(len(keys) * TEST_FRAC)
    test, train = keys[:n_test], keys[n_test:]
    g_train = build_graph(n, train, title_all, abs_all)
    validate(g_train, test, n, "v4 mega_fixed")

    log(f"\nOutputs in {OUT_DIR}/ . No Neo4j writes.")


if __name__ == "__main__":
    main()
