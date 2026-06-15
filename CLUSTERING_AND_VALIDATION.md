# H02P Clustering & Validation

*Alternative-to-IPC classification for motor-control patents, discovered by graph
community detection and validated against IPC.*

Last updated: 2026-06-15. Scope: IPC subclass **H02P**, countries **US + WO**,
**55,994** family representatives (one earliest-filing patent per family).

---

## 1. What the clustering does

Instead of accepting the hand-assigned IPC codes, we let the *structure of the
patent corpus* define its own technology communities. We build one weighted graph
over the 55,994 patents from three independent signals and run the **Leiden**
community-detection algorithm on it.

### The three signals (edges)

| Signal | Neo4j relationship | What it captures | Weight in graph |
|---|---|---|---|
| Citations | `CITES_PATENT` | "this patent built on that one" | `× 3.0` |
| Title similarity | `SIMILAR_TITLE` (`score`) | same wording in titles | `score × 0.4` |
| Abstract similarity | `SIMILAR_ABSTRACT` (`score`) | same concepts in abstracts | `score × 0.6` |

Weights are added per patent-pair, so a pair connected by all three signals gets a
strong combined edge. The weights are calibrated so each signal contributes a
comparable *total* pull per node, with citations favored slightly per-edge for
reliability. (Citations are sparse and US/WO-biased; the two similarity signals are
dense — ~100% / ~91% coverage — and rescue patents that would otherwise be isolated.)

### The algorithm

- **Leiden** (`leidenalg`, `RBConfigurationVertexPartition`), seed 42 (deterministic).
- Run at four **resolutions** γ, which trade off cluster size vs. number:
  - γ=0.3 → coarse "mega" communities
  - γ=0.5, γ=1.0 → mid-level "technology" communities
  - γ=2.0 → fine sub-communities
- Higher γ = more, smaller clusters.

### Why this evolved (v1 → v2 → v3)

The story is **singleton reduction** — patents with no in-scope edge can't be
clustered. Adding similarity signals rescued them:

| Version | Signals | Isolated patents (γ=0.3) |
|---|---|---|
| v1 | citations only | 28.2% |
| v2 | + title similarity | 7.4% |
| **v3** | + title + abstract | **2.7%** |

v3 is the current model. It produces ~5 mega-clusters and ~23 named sub-clusters
(EPS, aerospace starter/generators, HDD spindle BLDC, FOC induction, etc.), several
of which are **cross-IPC** technologies that have no dedicated IPC code.

**Code:** `cluster_h02p_v3.py` → outputs in `cluster_h02p_v3_out/`.
**Cluster naming evidence:** `name_clusters_v3.py` → `cluster_naming_data.json`
(top-10 most-central patents per cluster). Naming is currently manual; LLM
auto-labeling is a planned step.

---

## 2. The validation: held-out citation prediction

The open question was: **are these clusters actually better than IPC, or just a
different-looking grouping?** We now have a quantitative answer.

### The idea

A good technology grouping should put **citing and cited patents together** — if
patent A cites patent B, they are almost certainly about related technology. So we
test: *do cited pairs land in the same cluster more than chance, and more than IPC
achieves?*

### Avoiding circularity (the key methodological point)

v3 **uses** citations to build clusters (weight 3.0). Measuring citation containment
on those same edges would just measure what the algorithm optimized — meaningless.

So we do a proper **held-out test**:

1. Take all 130,096 in-scope citation pairs, randomly **hold out 20% (26,019 pairs)**.
2. **Re-cluster using only the other 80%** (+ all title/abstract edges).
3. Score the held-out 20% — pairs the model never saw.

IPC never used citations at all, so it is a **fair baseline** on the same held-out
edges.

### The metric: lift (size-aware)

- **Containment** = fraction of held-out cited pairs whose two patents are in the
  same cluster.
- **Null** = the containment you'd expect by pure chance, given the cluster-size
  distribution (`Σ nₖ(nₖ−1) / N(N−1)`).
- **Lift** = containment / null. Lift = 1 means "no better than random."

Lift is **size-aware**: a giant mega-cluster trivially contains many edges, but its
null is large too, so it gets no free credit. This directly neutralizes the
"M0 mega-blob absorbs everything" concern.

**Important:** lift naturally rises for *finer* partitions (smaller groups → smaller
null). So you **cannot** compare lift across rows with very different cluster counts.
The fair comparison is at **matched granularity** — see the figure.

---

## 3. Results

Held-out citation prediction, 26,019 test pairs, seed 1234:

| Method | #clusters | Containment | Lift |
|---|---|---|---|
| Leiden γ=0.3 | 1,814 | 81.3% | 2.4 |
| Leiden γ=0.5 | 1,821 | 71.2% | 3.5 |
| **Leiden γ=1.0** | **1,830** | **63.7%** | 6.6 |
| Leiden γ=2.0 | 1,838 | 50.6% | 18.5 |
| **IPC main-group** | **1,545** | **25.6%** | 6.3 |
| IPC full-group | 4,639 | 8.4% | 14.9 |
| Random (matched) | ~1,820 | 0.1% | ~1.0 |

*(#clusters counts every cluster including singletons, so mean size is not
informative; the null model uses the true size distribution.)*

### Headline

**At matched granularity, the clusters group cited patents ~2.5× more often than
IPC.** Compare the two ~1,500–1,800-group rows:

- Leiden γ=1.0: **63.7%** of held-out citations fall inside a cluster.
- IPC main-group: **25.6%**.

The **random baseline lands at lift ≈ 1.0** (containment 0.1%), confirming the
metric is honest and not an artifact.

### Figures

- `cluster_h02p_v3_out/validation_containment_vs_null.png` — the granularity-fair
  view: the Leiden γ-sweep traces a curve; the two IPC partitions are plotted as
  points. **IPC sits below the Leiden curve at every granularity**, i.e. clusters
  dominate IPC no matter the resolution you compare at.
- `cluster_h02p_v3_out/validation_matched_granularity.png` — the headline bar chart:
  Leiden γ=1.0 vs IPC main-group vs random, at comparable cluster counts.

This closes the prior gap ("no quantitative validation yet"). The defensible claim
is now: **held-out citations land in the same cluster ~2.5× more often than IPC
predicts, at equal granularity.**

---

## 4. Honest caveats

- **One downstream task so far.** Citation prediction is one signal. A second task
  (prior-art retrieval precision@k) would further strengthen the claim.
- **IPC partition assumption.** Each patent has multiple IPC codes; for a clean
  partition baseline we assign each its *first-listed* group. A multi-label IPC
  baseline could shift IPC's numbers somewhat (likely upward, but not enough to
  close a 2.5× gap).
- **Scope is US + WO only**, because citation coverage in the KG is US/WO-biased.
  Non-US/WO membership is an open problem.
- **`SIMILAR_TITLE` / `SIMILAR_ABSTRACT` are external inputs.** They are consumed by
  the clustering but built by a separate embedding job not in this repo. Scaling to
  a new IPC subclass requires running that job for the new scope first.
- **The mega→sub hierarchy is reconstructed, not nested.** It is built by majority-
  vote parenting of independent Leiden runs, not true hierarchical Leiden.

---

## 5. Files

| File | Purpose |
|---|---|
| `cluster_h02p_v3.py` | Build the v3 clustering (read-only Neo4j → JSON) |
| `name_clusters_v3.py` | Extract central patents per cluster for naming |
| `build_hierarchy_report.py` | Render the interactive `hierarchy_report_v3.html` |
| `validate_clustering.py` | Held-out citation-prediction validation (this doc §2–3) |
| `plot_validation.py` | Generate the two validation figures |
| `_validation_cache.pkl` | Cached Neo4j edges (so re-runs are seconds, not minutes) |
| `cluster_h02p_v3_out/validation_results.json` | Validation table (machine-readable) |
| `cluster_h02p_v3_out/validation_curve.json` | Dense γ-sweep + IPC points for the curve |

All validation is **read-only on local Neo4j — no BigQuery, no writes, no cloud cost.**

---

## 6. v4 — fixing the M0 over-merge (recursive nested Leiden)

v3's γ=0.3 mega level put **72% of the corpus (40,511 patents) in one blob** (M0),
merging EPS + FOC + ECM + sensorless-PM + braking + stepper + … into a single
top-level cluster. v4 fixes this with **recursive (nested) Leiden**: cluster at
γ=0.3, then re-cluster any mega larger than 15% of the corpus at γ=1.0, replacing
each oversized blob with coherent sub-megas (`cluster_h02p_v4.py`).

Result (full graph):

| | v3 mega (γ=0.3) | **v4 mega_fixed** |
|---|---|---|
| Largest cluster | 40,511 (72%) | **5,260 (9.4%)** |
| Meaningful megas (≥1%) | 1 | **26, named** |
| Held-out citation containment | 0.81 (blob artifact, lift 2.4) | **0.49 (lift 16.8)** |
| vs IPC main-group (0.256 / 6.3) | — | **1.9× containment, 2.7× lift** |
| Hierarchy | majority-vote (fake) | **true nested (parent_mega)** |

Fixing the blob did **not** cost validation strength — v4 still beats IPC ~1.9× on
held-out containment and adds a genuine 2-level hierarchy. The 26 megas are named
from their central patents (`name_clusters_v4.py` → `mega_naming_data.json`) and
pushed to Neo4j as `version:"v4"` (`push_clusters_v4_to_neo4j.py`), alongside v3.
v4 is the recommended taxonomy.

*(Method refinement: v4 de-duplicates reciprocal citation edges, so its γ=0.3
baseline reads 66% vs v3's 72% — same conclusion.)*

## 7. Next steps (in priority order)

1. **(done) Quantitative validation** — held-out citation prediction. ✓
2. **(done) Fix the M0 over-merge** — recursive nested Leiden → v4, 26 balanced megas. ✓
3. **(done) Cluster naming** — v4 megas named from central patents. ✓
4. **Second validation task** — prior-art retrieval precision@k.
5. **Build a v4 hierarchy report** (the HTML viz currently renders v1/v2/v3).
6. **Then** scale to other subclasses (recommended approach: one global graph with
   no IPC pre-scoping, letting communities cross IPC boundaries).
