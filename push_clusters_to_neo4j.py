"""
Push the v3 clustering RESULTS into Neo4j as first-class Cluster nodes.

Model (versioned, non-destructive, reversible):
    (:Cluster {key, version, level, gamma, cluster_id, size, name})
    (:Patent)-[:IN_CLUSTER]->(:Cluster)

- key = "<version>:<level>:<cluster_id>"  (unique -> idempotent MERGE)
- Only the 55,994 family representatives get IN_CLUSTER edges.
- Names attached for the mega level (coarse_03) and sub level (l2) where known.
- Re-runnable: MERGE everywhere, so running twice changes nothing.
- Rollback:  MATCH (c:Cluster {version:"v3"}) DETACH DELETE c

Run:
    /home/yacoubcherouali/kg-builder/venv/bin/python push_clusters_to_neo4j.py
"""
import json
import os
import time
from collections import Counter, defaultdict
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "cluster_h02p_v3_out"

load_dotenv(SCRIPT_DIR / ".env")
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PWD = os.getenv("NEO4J_PASSWORD")

VERSION = "v3"
GAMMA = {"coarse_03": 0.3, "l1": 0.5, "l2": 1.0, "l3": 2.0}
MEGA_LEVEL = "coarse_03"
SUB_LEVEL = "l2"
BATCH = 5000

# Hand-curated names (mirrors build_hierarchy_report.py for v3)
MEGA_NAMES = {
    0: "Electronically Commutated Motors & Inverter-Fed AC Drives",
    1: "Variable-Speed Generators: Wind, Hydro & Aerospace Power",
    2: "Single-Phase Induction Motor Starting Circuits",
    3: "Vintage Industrial Motor Control (Westinghouse-era, 1910s-1920s)",
    4: "SiC Power Semiconductors for Traction & Elevator Inverters",
    5: "Foldable Smartphone Flexible-Display Drive Motors",
}
SUB_NAMES = {
    0: "Electric Power Steering — Vector & Torque Control",
    1: "Aerospace Starter-Generators (VSCF Brushless)",
    2: "Vintage Heavy-Industrial DC Motor Control (1920s-1950s)",
    3: "HVAC Compressor & Refrigeration Inverter Drives",
    4: "Sensorless BLDC Spindle Control (HDD & Disk Drives)",
    5: "Field-Oriented Vector Control of AC Induction Motors",
    6: "Electronic Braking for Power Tools & AC Drives",
    7: "Electronically Commutated Motor (ECM) Drive Systems",
    8: "Variable-Speed Wind Turbine Generators",
    9: "Single-Phase AC Motor Starting (Auxiliary Winding)",
    10: "Stepper Motor Drivers (Industrial & Watch Movement)",
    11: "Switched Reluctance Motor Position Sensing",
    12: "Vintage Generator Voltage Regulation (Westinghouse, 1930s-1940s)",
    13: "Door & Window Motor Operators (Garage & Vehicle Windows)",
    14: "Surgical Instrument Motor Control (Staplers & Robotic Surgery)",
    15: "Linear Reciprocating Motors (Compressors & Vibration Actuators)",
    16: "Vintage Switchboard Motor Control (Westinghouse, 1910s-1920s)",
    17: "Pulsed Electric Machine Control (Tula Technology)",
    18: "SiC Power Devices for Traction Inverters (Toshiba)",
    19: "Motor Fault Diagnosis & Condition Monitoring",
    20: "On-Load Tap Changers for Power Transformers",
    21: "Vintage Industrial Motor Controllers (Cutler-Hammer, 1910s-1950s)",
    22: "UAV Electronic Speed Controllers (Drone BLDC)",
}
NAMES_BY_LEVEL = {MEGA_LEVEL: MEGA_NAMES, SUB_LEVEL: SUB_NAMES}


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def main():
    with open(OUT_DIR / "clusters.json") as f:
        clusters = json.load(f)  # {pub: {level: cluster_id}}
    log(f"loaded {len(clusters):,} rep assignments")

    levels = list(GAMMA.keys())

    # cluster sizes per (level, cluster_id)
    sizes = {lvl: Counter() for lvl in levels}
    for pub, assign in clusters.items():
        for lvl in levels:
            if lvl in assign:
                sizes[lvl][assign[lvl]] += 1

    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    with driver.session() as s:
        # 0) safety constraint (idempotent)
        s.run("CREATE CONSTRAINT cluster_key IF NOT EXISTS "
              "FOR (c:Cluster) REQUIRE c.key IS UNIQUE")

        # 1) Cluster nodes
        cluster_rows = []
        for lvl in levels:
            for cid, size in sizes[lvl].items():
                cluster_rows.append({
                    "key": f"{VERSION}:{lvl}:{cid}",
                    "version": VERSION, "level": lvl, "gamma": GAMMA[lvl],
                    "cluster_id": int(cid), "size": int(size),
                    "name": NAMES_BY_LEVEL.get(lvl, {}).get(int(cid)),
                    "is_mega": lvl == MEGA_LEVEL, "is_sub": lvl == SUB_LEVEL,
                })
        log(f"upserting {len(cluster_rows):,} Cluster nodes across {len(levels)} levels...")
        q_node = """
        UNWIND $rows AS row
        MERGE (c:Cluster {key: row.key})
        SET c.version=row.version, c.level=row.level, c.gamma=row.gamma,
            c.cluster_id=row.cluster_id, c.size=row.size, c.name=row.name,
            c.is_mega=row.is_mega, c.is_sub=row.is_sub
        """
        for i in range(0, len(cluster_rows), BATCH):
            s.run(q_node, rows=cluster_rows[i:i+BATCH])

        # 2) IN_CLUSTER edges (rep -> cluster), one per level
        edge_rows = []
        for pub, assign in clusters.items():
            for lvl in levels:
                if lvl in assign:
                    edge_rows.append({"pub": pub, "key": f"{VERSION}:{lvl}:{assign[lvl]}"})
        log(f"upserting {len(edge_rows):,} IN_CLUSTER edges...")
        q_edge = """
        UNWIND $rows AS row
        MATCH (p:Patent {publication_number: row.pub})
        MATCH (c:Cluster {key: row.key})
        MERGE (p)-[:IN_CLUSTER]->(c)
        """
        t0 = time.time()
        matched = 0
        for i in range(0, len(edge_rows), BATCH):
            s.run(q_edge, rows=edge_rows[i:i+BATCH])
            matched += len(edge_rows[i:i+BATCH])
            if (i // BATCH) % 4 == 0:
                log(f"  {matched:,}/{len(edge_rows):,} edges ({time.time()-t0:.0f}s)")

        # 3) verify
        n_clusters = s.run("MATCH (c:Cluster {version:$v}) RETURN count(c) AS n",
                           v=VERSION).single()["n"]
        n_edges = s.run(
            "MATCH (:Patent)-[r:IN_CLUSTER]->(c:Cluster {version:$v}) RETURN count(r) AS n",
            v=VERSION).single()["n"]
        n_patents = s.run(
            "MATCH (p:Patent)-[:IN_CLUSTER]->(c:Cluster {version:$v}) "
            "RETURN count(DISTINCT p) AS n", v=VERSION).single()["n"]
        log(f"VERIFY: {n_clusters:,} Cluster nodes | {n_edges:,} IN_CLUSTER edges | "
            f"{n_patents:,} distinct patents")
    driver.close()
    log("done. Rollback if needed: MATCH (c:Cluster {version:'v3'}) DETACH DELETE c")


if __name__ == "__main__":
    main()
