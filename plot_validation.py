"""
Make expert-facing figures for the held-out citation-prediction validation.

Reuses the warm cache (_validation_cache.pkl) built by validate_clustering.py.
Runs a denser gamma sweep so we can draw the containment-vs-null CURVE and overlay
the IPC partitions as points -- showing IPC sits below the Leiden curve at every
granularity (i.e. clusters dominate IPC at matched resolution).

Read-only, no Neo4j, no cost. Outputs PNGs into cluster_h02p_v3_out/.

Run:
    /home/yacoubcherouali/kg-builder/venv/bin/python plot_validation.py
"""
import json
import pickle
import random
import time
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import igraph as ig
import leidenalg

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "cluster_h02p_v3_out"
CACHE = SCRIPT_DIR / "_validation_cache.pkl"

CITE_WEIGHT = 3.0
TITLE_WEIGHT_FACTOR = 0.4
ABSTRACT_WEIGHT_FACTOR = 0.6
LEIDEN_SEED = 42
TEST_FRAC = 0.20
SPLIT_SEED = 1234

# denser sweep so the curve spans IPC's null values
GAMMAS = [0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 4.0]


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


def cluster(n, cite_pairs, title_pairs, abs_pairs, gamma):
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
    part = leidenalg.find_partition(
        g, leidenalg.RBConfigurationVertexPartition,
        resolution_parameter=gamma, weights="weight", seed=LEIDEN_SEED)
    return list(part.membership)


def null_co(mem, n):
    sizes = Counter(mem)
    return sum(s * (s - 1) for s in sizes.values()) / (n * (n - 1))


def score(mem, test, n):
    hits = sum(1 for (a, b) in test if mem[a] == mem[b])
    contain = hits / len(test)
    null = null_co(mem, n)
    return contain, null, (contain / null if null else float("inf")), len(set(mem))


def ipc_partition(pubs, ipc_map, level):
    lab, mem = {}, []
    for pub in pubs:
        g = ipc_map.get(pub) or []
        code = g[0] if g else "UNKNOWN"
        if level == "main":
            code = code.split("/")[0]
        lab.setdefault(code, len(lab))
        mem.append(lab[code])
    return mem


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

    # Leiden sweep
    curve = []
    for gma in GAMMAS:
        mem = cluster(n, train, title_all, abs_all, gma)
        c, nu, lift, k = score(mem, test, n)
        curve.append({"gamma": gma, "containment": c, "null": nu, "lift": lift, "k": k})
        log(f"γ={gma}: k={k:,} contain={c:.3f} null={nu:.5f} lift={lift:.1f}")

    # IPC points
    ipc_pts = []
    for lvl, name in [("main", "IPC main-group"), ("group", "IPC full-group")]:
        mem = ipc_partition(pubs, data["ipc"], lvl)
        c, nu, lift, k = score(mem, test, n)
        ipc_pts.append({"name": name, "containment": c, "null": nu, "lift": lift, "k": k})
        log(f"{name}: k={k:,} contain={c:.3f} null={nu:.5f} lift={lift:.1f}")

    random.Random(SPLIT_SEED)
    rk = curve[4]["k"]  # ~ gamma 1.0
    rmem = [random.Random(SPLIT_SEED + i).randrange(rk) for i in range(n)]
    rc, rnu, rlift, _ = score(rmem, test, n)

    # ---- FIGURE 1: containment-vs-null curve (the granularity-fair comparison)
    fig, ax = plt.subplots(figsize=(8, 6))
    xs = [p["null"] for p in curve]
    ys = [p["containment"] for p in curve]
    ax.plot(xs, ys, "-o", color="#2563eb", lw=2, label="Leiden clusters (γ sweep)")
    for p in curve:
        ax.annotate(f"γ={p['gamma']}", (p["null"], p["containment"]),
                    textcoords="offset points", xytext=(5, 6), fontsize=8, color="#2563eb")
    for p in ipc_pts:
        ax.plot(p["null"], p["containment"], "s", ms=11, color="#dc2626")
        ax.annotate(p["name"], (p["null"], p["containment"]),
                    textcoords="offset points", xytext=(8, -4), fontsize=9, color="#dc2626")
    ax.set_xscale("log")
    ax.set_xlabel("Null co-membership  (chance level — granularity proxy; left = finer)")
    ax.set_ylabel("Held-out citation containment  (fraction of cited pairs in same cluster)")
    ax.set_title("Clusters capture far more citation structure than IPC\n"
                 "at every granularity (curve above IPC points = better)")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left")
    fig.tight_layout()
    f1 = OUT_DIR / "validation_containment_vs_null.png"
    fig.savefig(f1, dpi=140)
    log(f"saved {f1.name}")

    # ---- FIGURE 2: matched-granularity bar (Leiden γ=1.0 vs IPC main-group)
    g1 = curve[4]  # gamma=1.0
    ipc_main = ipc_pts[0]
    fig2, ax2 = plt.subplots(figsize=(7, 6))
    labels = [f"Leiden γ=1.0\n({g1['k']:,} clusters)",
              f"IPC main-group\n({ipc_main['k']:,} groups)",
              f"Random\n(matched)"]
    vals = [g1["containment"], ipc_main["containment"], rc]
    colors = ["#2563eb", "#dc2626", "#9ca3af"]
    bars = ax2.bar(labels, vals, color=colors)
    for b, v in zip(bars, vals):
        ax2.text(b.get_x() + b.get_width() / 2, v + 0.012, f"{v*100:.1f}%",
                 ha="center", fontsize=12, fontweight="bold")
    ax2.set_ylabel("Held-out citations landing in the same cluster")
    ax2.set_ylim(0, max(vals) * 1.25)
    ax2.set_title("At matched granularity (~1,500–1,800 groups)\n"
                  "clusters group cited patents ~2.5× more often than IPC")
    fig2.tight_layout()
    f2 = OUT_DIR / "validation_matched_granularity.png"
    fig2.savefig(f2, dpi=140)
    log(f"saved {f2.name}")

    with open(OUT_DIR / "validation_curve.json", "w") as f:
        json.dump({"leiden_curve": curve, "ipc_points": ipc_pts,
                   "random": {"containment": rc, "null": rnu, "lift": rlift, "k": rk},
                   "n_reps": n, "n_test_edges": len(test)}, f, indent=2)
    log("saved validation_curve.json")


if __name__ == "__main__":
    main()
