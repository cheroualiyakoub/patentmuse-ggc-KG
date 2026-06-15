"""
Push the v4 (recursive-nested, M0-fixed) clustering into Neo4j, versioned 'v4',
alongside v3. Non-destructive, idempotent (MERGE), reversible.

Levels pushed:
  - 'mega_fixed' : the balanced top taxonomy (26 named megas), with parent_mega provenance
  - 'l2'         : the γ=1.0 sub level (for drill-down; unnamed for now)

Model: (:Cluster {key, version:'v4', level, cluster_id, size, name, parent_mega})
       (:Patent)-[:IN_CLUSTER]->(:Cluster)
key = "v4:<level>:<cluster_id>".
Rollback: MATCH (c:Cluster {version:'v4'}) DETACH DELETE c

Run from project dir:
    /home/yacoubcherouali/kg-builder/venv/bin/python push_clusters_v4_to_neo4j.py
"""
import json
import os
import time
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "cluster_h02p_v4_out"
load_dotenv(SCRIPT_DIR / ".env")
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PWD = os.getenv("NEO4J_PASSWORD")

VERSION = "v4"
LEVELS = ["mega_fixed", "l2"]
BATCH = 5000

MEGA_FIXED_NAMES = {
    0:    "Sensorless Rotor-Position Estimation (PM/AC Synchronous)",
    1755: "Electronically Commutated & Sensorless Brushless-DC Commutation",
    1748: "DC Motor Speed Control (Armature & Field Regulation)",
    1757: "Electric Power Steering Motor Control",
    1756: "Field-Oriented (Vector) Control of AC Induction Motors",
    1750: "Dynamic & Electronic Braking (Power Tools & AC Drives)",
    1752: "Phase-Locked Precision Speed Control (Servo/Drives)",
    2:    "Single-Phase Induction Motor Starting Circuits",
    1:    "Vintage Industrial Control & Regulating Systems (Westinghouse-era)",
    1754: "Stepper Motor Drivers (Pumps, Printers, Timepieces)",
    1749: "Pulsed Electric Machine Control (Tula)",
    1751: "H-Bridge Bidirectional DC Drives (Vehicle Actuators)",
    1753: "Inverter Motor Drives for Refrigeration & Appliances",
    1706: "Variable-Speed Wind Turbine Generators (DFIG/VSCF)",
    1719: "Automotive Alternator Voltage Regulation",
    1758: "Brushless Power-Tool & Surgical-Instrument Motor Control",
    1716: "Hybrid/EV Traction Inverters & Integrated Charging",
    1759: "AC Induction Soft-Starters & Power-Factor Control",
    1763: "Switched Reluctance Motor Control & Position Sensing",
    1709: "Direct-Drive Wind & Wave Energy Generators",
    1712: "Aerospace Brushless Starter-Generators (VSCF Exciter)",
    1714: "Engine-Driven Genset Excitation & Voltage Regulation",
    1762: "Brushless-DC Fan & HVAC Blower Control",
    1705: "Aircraft Propeller/Engine Speed Synchronization (Vintage)",
    1715: "Variable Coil-Configuration / Winding-Switching Machines",
    1711: "Microturbine / Turbogenerator Power Control",
}


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def main():
    clusters = json.load(open(OUT_DIR / "clusters.json"))
    parent = json.load(open(OUT_DIR / "mega_fixed_parent.json"))  # {final_id: base_mega}
    log(f"loaded {len(clusters):,} assignments")

    sizes = {lvl: Counter() for lvl in LEVELS}
    for pub, a in clusters.items():
        for lvl in LEVELS:
            if lvl in a:
                sizes[lvl][a[lvl]] += 1

    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    with driver.session() as s:
        s.run("CREATE CONSTRAINT cluster_key IF NOT EXISTS "
              "FOR (c:Cluster) REQUIRE c.key IS UNIQUE")

        nodes = []
        for lvl in LEVELS:
            for cid, size in sizes[lvl].items():
                name = MEGA_FIXED_NAMES.get(int(cid)) if lvl == "mega_fixed" else None
                nodes.append({
                    "key": f"{VERSION}:{lvl}:{cid}", "version": VERSION, "level": lvl,
                    "cluster_id": int(cid), "size": int(size), "name": name,
                    "parent_mega": parent.get(str(cid)) if lvl == "mega_fixed" else None,
                    "method": "recursive_leiden" if lvl == "mega_fixed" else "leiden",
                })
        log(f"upserting {len(nodes):,} Cluster nodes (v4)...")
        s.run("""
        UNWIND $rows AS r
        MERGE (c:Cluster {key:r.key})
        SET c.version=r.version, c.level=r.level, c.cluster_id=r.cluster_id,
            c.size=r.size, c.name=r.name, c.parent_mega=r.parent_mega, c.method=r.method,
            c.is_mega = (r.level='mega_fixed'), c.is_sub = (r.level='l2')
        """, rows=nodes)

        edges = []
        for pub, a in clusters.items():
            for lvl in LEVELS:
                if lvl in a:
                    edges.append({"pub": pub, "key": f"{VERSION}:{lvl}:{a[lvl]}"})
        log(f"upserting {len(edges):,} IN_CLUSTER edges (v4)...")
        t0 = time.time()
        for i in range(0, len(edges), BATCH):
            s.run("""
            UNWIND $rows AS r
            MATCH (p:Patent {publication_number:r.pub})
            MATCH (c:Cluster {key:r.key})
            MERGE (p)-[:IN_CLUSTER]->(c)
            """, rows=edges[i:i+BATCH])
        log(f"  edges done ({time.time()-t0:.0f}s)")

        nc = s.run("MATCH (c:Cluster {version:'v4'}) RETURN count(c) AS n").single()["n"]
        ne = s.run("MATCH (:Patent)-[r:IN_CLUSTER]->(:Cluster {version:'v4'}) RETURN count(r) AS n").single()["n"]
        named = s.run("MATCH (c:Cluster {version:'v4',level:'mega_fixed'}) WHERE c.name IS NOT NULL RETURN count(c) AS n").single()["n"]
        biggest = s.run("MATCH (c:Cluster {version:'v4',level:'mega_fixed'}) RETURN max(c.size) AS m").single()["m"]
        log(f"VERIFY: {nc:,} Cluster nodes | {ne:,} edges | {named} named megas | largest mega={biggest:,}")
    driver.close()
    log("done. Rollback: MATCH (c:Cluster {version:'v4'}) DETACH DELETE c")


if __name__ == "__main__":
    main()
