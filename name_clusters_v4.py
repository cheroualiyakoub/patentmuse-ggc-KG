"""
Extract naming evidence for v4 mega_fixed clusters: the top-K most central patents
(highest in-cluster weighted degree) per meaningful mega, with title/assignees/IPC
fetched from Neo4j for just those patents. Output -> cluster_h02p_v4_out/mega_naming_data.json

Read-only. Run from project dir:
    /home/yacoubcherouali/kg-builder/venv/bin/python name_clusters_v4.py
"""
import json
import os
import pickle
import time
from collections import Counter, defaultdict
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "cluster_h02p_v4_out"
CACHE = SCRIPT_DIR / "_validation_cache.pkl"
load_dotenv(SCRIPT_DIR / ".env")
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PWD = os.getenv("NEO4J_PASSWORD")

CITE_WEIGHT, TITLE_F, ABS_F = 3.0, 0.4, 0.6
TOP_K = 10
MEANINGFUL = 560


def upairs(directed, p2i, weighted):
    out = {}
    for e in directed:
        s, t = e[0], e[1]
        if s in p2i and t in p2i and s != t:
            a, b = sorted((p2i[s], p2i[t]))
            out[(a, b)] = max(out.get((a, b), 0.0), e[2]) if weighted else 1.0
    return out


def main():
    with open(CACHE, "rb") as f:
        data = pickle.load(f)
    pubs = data["pubs"]
    p2i = {p: i for i, p in enumerate(pubs)}
    clusters = json.load(open(OUT_DIR / "clusters.json"))
    mega = {p2i[pub]: a["mega_fixed"] for pub, a in clusters.items() if pub in p2i}

    # weighted edges
    ew = defaultdict(float)
    for (a, b) in upairs(data["cite"], p2i, False):
        ew[(a, b)] += CITE_WEIGHT
    for (a, b), s in upairs(data["title"], p2i, True).items():
        ew[(a, b)] += s * TITLE_F
    for (a, b), s in upairs(data["abstract"], p2i, True).items():
        ew[(a, b)] += s * ABS_F

    # in-cluster weighted degree (single pass)
    deg = defaultdict(float)
    for (a, b), w in ew.items():
        if mega.get(a) == mega.get(b):
            deg[a] += w
            deg[b] += w

    cluster_members = defaultdict(list)
    for idx, c in mega.items():
        cluster_members[c].append(idx)

    big = {c: m for c, m in cluster_members.items() if len(m) >= MEANINGFUL}
    print(f"{len(big)} meaningful megas")

    # top central pubs per mega
    central = {}
    need = set()
    for c, members in big.items():
        top = sorted(members, key=lambda i: -deg.get(i, 0))[:TOP_K]
        central[c] = top
        for i in top:
            need.add(pubs[i])

    # fetch details for just those ~260 patents
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    details = {}
    with driver.session() as s:
        q = """
        UNWIND $pubs AS pub
        MATCH (p:Patent {publication_number: pub})
        OPTIONAL MATCH (p)-[:ASSIGNED_TO]->(a:Assignee)
        OPTIONAL MATCH (p)-[:CLASSIFIED_AS]->(g:IPC_Group)
        RETURN pub, p.title AS title, substring(coalesce(p.abstract,''),0,300) AS abstract,
               collect(DISTINCT a.name)[0..3] AS assignees,
               collect(DISTINCT g.code)[0..5] AS ipc
        """
        for r in s.run(q, pubs=list(need)):
            details[r["pub"]] = {"title": r["title"], "abstract": r["abstract"],
                                 "assignees": r["assignees"], "ipc": r["ipc"]}
    driver.close()

    out = []
    for c, members in sorted(big.items(), key=lambda x: -len(x[1])):
        out.append({
            "cluster_id": int(c), "size": len(members),
            "top_central": [{"pub": pubs[i], "degree": round(deg.get(i, 0), 1),
                             **details.get(pubs[i], {})} for i in central[c]],
        })
    json.dump(out, open(OUT_DIR / "mega_naming_data.json", "w"), indent=2, default=str)
    print(f"wrote mega_naming_data.json ({len(out)} megas)")


if __name__ == "__main__":
    main()
