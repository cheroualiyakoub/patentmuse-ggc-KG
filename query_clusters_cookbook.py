"""
Tested retrieval cookbook for the v3 clusters in Neo4j.
Runs each query and prints the Cypher + real results. Read-only.

Run: /home/yacoubcherouali/kg-builder/venv/bin/python query_clusters_cookbook.py
"""
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv(".env")
d = GraphDatabase.driver(os.getenv("NEO4J_URI", "bolt://localhost:7687"),
                         auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")))


def show(title, cypher, params=None, fmt=None, limit_print=8):
    print("\n" + "=" * 78)
    print(f"# {title}")
    print("-" * 78)
    print(cypher.strip())
    print("-" * 78)
    with d.session() as s:
        rows = list(s.run(cypher, **(params or {})))
    if not rows:
        print("  (no rows)")
        return
    for r in rows[:limit_print]:
        print("  " + (fmt(r) if fmt else str(r.data())))
    if len(rows) > limit_print:
        print(f"  ... (+{len(rows)-limit_print} more)")


# 1. What levels/versions exist + how many clusters each
show("1. Inventory: what cluster levels exist",
     """
     MATCH (c:Cluster {version:'v3'})
     RETURN c.level AS level, c.gamma AS gamma, count(*) AS n_clusters,
            max(c.size) AS biggest, sum(c.size) AS patents_assigned
     ORDER BY gamma
     """,
     fmt=lambda r: f"level={r['level']:10s} γ={r['gamma']}  clusters={r['n_clusters']:>5}  "
                   f"biggest={r['biggest']:>6,}  assigned={r['patents_assigned']:>7,}")

# 2. All named MEGA clusters
show("2. Named mega-clusters (γ=0.3) by size",
     """
     MATCH (c:Cluster {version:'v3', level:'coarse_03'})
     WHERE c.name IS NOT NULL
     RETURN c.cluster_id AS id, c.size AS size, c.name AS name
     ORDER BY size DESC
     """,
     fmt=lambda r: f"M{r['id']:<3} {r['size']:>6,}  {r['name']}")

# 3. All named SUB clusters
show("3. Named sub-clusters (γ=1.0) by size",
     """
     MATCH (c:Cluster {version:'v3', level:'l2'})
     WHERE c.name IS NOT NULL
     RETURN c.cluster_id AS id, c.size AS size, c.name AS name
     ORDER BY size DESC
     """,
     fmt=lambda r: f"S{r['id']:<3} {r['size']:>6,}  {r['name']}", limit_print=25)

# 4. Patents inside a named cluster (keyword search on name)
show("4. Patents in a cluster found by keyword ('Wind Turbine')",
     """
     MATCH (c:Cluster {version:'v3', level:'l2'})
     WHERE toLower(c.name) CONTAINS 'wind turbine'
     MATCH (p:Patent)-[:IN_CLUSTER]->(c)
     RETURN p.publication_number AS pub, p.filing_date AS filed, p.title AS title
     ORDER BY filed DESC
     """,
     fmt=lambda r: f"{r['pub']:18s} {str(r['filed'])[:10]}  {(r['title'] or '')[:55]}")

# 5. Which clusters does ONE patent belong to (all levels)?
show("5. Cluster membership of a single patent (all 4 levels)",
     """
     MATCH (p:Patent)-[:IN_CLUSTER]->(c:Cluster {version:'v3'})
     WHERE p.publication_number STARTS WITH 'US-' AND c.level='l2' AND c.name IS NOT NULL
     WITH p LIMIT 1
     MATCH (p)-[:IN_CLUSTER]->(c:Cluster {version:'v3'})
     RETURN p.publication_number AS pub, c.level AS level, c.gamma AS gamma,
            c.cluster_id AS cid, c.size AS size, c.name AS name
     ORDER BY gamma
     """,
     fmt=lambda r: f"{r['pub']:16s} {r['level']:10s}(γ{r['gamma']}) id={r['cid']:<5} "
                   f"size={r['size']:>6,}  {r['name'] or '(unnamed)'}")

# 6. Top assignees in a named cluster
show("6. Top assignees in the EPS sub-cluster",
     """
     MATCH (c:Cluster {version:'v3', level:'l2'})
     WHERE c.name CONTAINS 'Electric Power Steering'
     MATCH (p:Patent)-[:IN_CLUSTER]->(c)
     MATCH (p)-[:ASSIGNED_TO]->(a:Assignee)
     RETURN a.name AS assignee, count(*) AS patents
     ORDER BY patents DESC
     """,
     fmt=lambda r: f"{r['patents']:>5,}  {r['assignee']}")

# 7. Mega -> sub hierarchy (how megas split into named subs)
show("7. Hierarchy: which sub-clusters sit under mega M0",
     """
     MATCH (p:Patent)-[:IN_CLUSTER]->(mega:Cluster {version:'v3', level:'coarse_03', cluster_id:0}),
           (p)-[:IN_CLUSTER]->(sub:Cluster {version:'v3', level:'l2'})
     WHERE sub.name IS NOT NULL
     RETURN sub.cluster_id AS sid, sub.name AS name, count(p) AS shared
     ORDER BY shared DESC
     """,
     fmt=lambda r: f"S{r['sid']:<3} {r['shared']:>6,}  {r['name']}")

# 8. Cross-cluster citation flow (which techs cite which)
show("8. Citation flow BETWEEN named sub-clusters (top influence pairs)",
     """
     MATCH (a:Patent)-[:IN_CLUSTER]->(ca:Cluster {version:'v3', level:'l2'}),
           (a)-[:CITES_PATENT]->(b:Patent)-[:IN_CLUSTER]->(cb:Cluster {version:'v3', level:'l2'})
     WHERE ca.name IS NOT NULL AND cb.name IS NOT NULL AND ca <> cb
     RETURN ca.name AS from_tech, cb.name AS to_tech, count(*) AS citations
     ORDER BY citations DESC
     """,
     fmt=lambda r: f"{r['citations']:>5,}  {r['from_tech'][:32]:32s} -> {r['to_tech'][:32]}")

# 9. Self-containment: how 'tight' is each named sub-cluster
#    (share of a cluster's citations that stay inside it)
show("9. Cluster cohesion: % of each sub-cluster's citations that stay internal",
     """
     MATCH (p:Patent)-[:IN_CLUSTER]->(c:Cluster {version:'v3', level:'l2'})
     WHERE c.name IS NOT NULL
     MATCH (p)-[:CITES_PATENT]->(q:Patent)
     WITH c, count(*) AS total,
          sum(CASE WHEN (q)-[:IN_CLUSTER]->(c) THEN 1 ELSE 0 END) AS internal
     RETURN c.cluster_id AS id, c.size AS size, total AS cites,
            round(100.0*internal/total,1) AS pct_internal, c.name AS name
     ORDER BY pct_internal DESC
     """,
     fmt=lambda r: f"S{r['id']:<3} cohesion={r['pct_internal']:>5}%  (n={r['size']:>5,}, "
                   f"{r['cites']:>5,} cites)  {r['name'][:40]}")

# 10. Time profile of a cluster (when was this tech active?)
show("10. Filing-year profile of the UAV ESC (drone) sub-cluster",
     """
     MATCH (c:Cluster {version:'v3', level:'l2'})
     WHERE c.name CONTAINS 'UAV'
     MATCH (p:Patent)-[:IN_CLUSTER]->(c)
     WHERE p.filing_date IS NOT NULL
     RETURN substring(toString(p.filing_date),0,4) AS year, count(*) AS patents
     ORDER BY year
     """,
     fmt=lambda r: f"{r['year']}: {'#'*r['patents']} ({r['patents']})")

d.close()
print("\n" + "=" * 78)
print("Done. All read-only. Copy any query above into Neo4j Browser / your app.")
