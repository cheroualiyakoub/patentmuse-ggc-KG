"""
Build a shareable HTML report combining the γ=0.3 mega-clusters with the γ=1.0
sub-clusters into a single hierarchical taxonomy.

Outputs a standalone HTML at cluster_h02p_v1_out/hierarchy_report.html

Run:
    /home/yacoubcherouali/kg-builder/venv/bin/python build_hierarchy_report.py
"""
import json
import os
from collections import Counter, defaultdict
from html import escape
from pathlib import Path

import plotly.graph_objects as go

# Switch between v1 (citation-only) and v2 (citation + SIMILAR_TITLE).
# Set CLUSTER_VERSION=v1 env var to regenerate the v1 report.
CLUSTER_VERSION = os.environ.get("CLUSTER_VERSION", "v2")

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / f"cluster_h02p_{CLUSTER_VERSION}_out"

MEGA_LEVEL = "coarse_03"   # γ=0.3
SUB_LEVEL = "l2"           # γ=1.0

MIN_MEGA_SIZE = 50         # below this, lump as "Other / specialty niches"
MIN_SUB_SIZE = 20          # below this, lump as "Other (small lineages)"

# Tentative human-readable names per mega-cluster ID. IDs are leiden-assigned
# and differ between v1 and v2 runs — we keep separate dicts per version.
MEGA_NAMES_BY_VERSION = {
    "v1": {
        0: "Modern BLDC / AC electronics / inverter + vector control",
        1: "Synchronous machines, generators, inverter-fed industrial drives",
        2: "Classical DC motor control + braking",
        3: "Motor starting + protection (appliance motors)",
        4: "Marine / wave / wind / hydroelectric generators",
    },
    "v2": {
        0: "Modern AC motor control — inverter + vector + BLDC",
        1: "Generators, wind power, aerospace electrical systems",
        2: "Classical DC motor control + protection",
        3: "Linear / voice-coil motors — mobile haptics & audio",
        4: "Construction & mining equipment electric drives",
        5: "Vintage single-phase motor starting (Westinghouse-era)",
    },
    "v3": {
        0: "Electronically Commutated Motors & Inverter-Fed AC Drives",
        1: "Variable-Speed Generators: Wind, Hydro & Aerospace Power",
        2: "Single-Phase Induction Motor Starting Circuits",
        3: "Vintage Industrial Motor Control (Westinghouse-era, 1910s-1920s)",
        4: "SiC Power Semiconductors for Traction & Elevator Inverters",
        5: "Foldable Smartphone Flexible-Display Drive Motors",
    },
}
MEGA_NAMES = MEGA_NAMES_BY_VERSION[CLUSTER_VERSION]

# Manually assigned sub-cluster names per version. Derived from the top-10
# central patents of each cluster (highest in-cluster weighted degree across
# citation + title + abstract similarity edges). Each name reflects the
# dominant technology + the assignee fingerprint visible in the central docs.
SUB_NAMES_BY_VERSION = {
    "v3": {
        0:  "Electric Power Steering — Vector & Torque Control",
        1:  "Aerospace Starter-Generators (VSCF Brushless)",
        2:  "Vintage Heavy-Industrial DC Motor Control (1920s-1950s)",
        3:  "HVAC Compressor & Refrigeration Inverter Drives",
        4:  "Sensorless BLDC Spindle Control (HDD & Disk Drives)",
        5:  "Field-Oriented Vector Control of AC Induction Motors",
        6:  "Electronic Braking for Power Tools & AC Drives",
        7:  "Electronically Commutated Motor (ECM) Drive Systems",
        8:  "Variable-Speed Wind Turbine Generators",
        9:  "Single-Phase AC Motor Starting (Auxiliary Winding)",
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
    },
}
SUB_NAMES = SUB_NAMES_BY_VERSION.get(CLUSTER_VERSION, {})

# Maps IPC group prefix → human-readable technology description.
# Covers H02P (motor control) plus the most common cross-cuts seen in clusters
# (power electronics, EVs, wind, etc.). Used to name sub-clusters automatically.
IPC_DESCRIPTIONS = {
    # H02P — motor control
    "H02P1":  "Motor starting",
    "H02P3":  "Motor stopping / braking",
    "H02P4":  "Reversing of motors",
    "H02P5":  "Motor speed control",
    "H02P6":  "BLDC / electronically commutated motor control",
    "H02P7":  "DC motor control",
    "H02P8":  "Stepper motor control",
    "H02P9":  "Generator / dynamo control",
    "H02P21": "Vector control / field-oriented control (FOC)",
    "H02P23": "Speed & position control of AC motors",
    "H02P25": "Variable-reluctance / synchronous reluctance motors",
    "H02P27": "PWM / inverter-fed motor control",
    "H02P29": "Motor protection & overload",
    "H02P31": "Arrangements for testing motors",
    # H02M — power electronics
    "H02M1":  "Power converter circuits — fundamentals",
    "H02M3":  "DC-DC converters",
    "H02M5":  "AC-AC converters",
    "H02M7":  "DC-AC inverters",
    # H02K — machines
    "H02K1":  "Machine cores & magnets",
    "H02K7":  "Motor structural arrangements",
    "H02K11": "Motors with integrated electronics",
    # Cross-cuts
    "F03D9":  "Wind turbine power conversion",
    "F03B13": "Hydraulic / wave power machines",
    "F03B3":  "Hydraulic turbines",
    "B60L":   "Electric vehicle propulsion",
    "B60T":   "Vehicle braking",
    "G05B":   "Generic control systems",
    "G05D":   "Position / speed control",
    "H03K17": "Switching circuits",
    "H05K1":  "PCBs / circuit boards",
    "F04B":   "Pumps",
    "F16D":   "Mechanical clutches & brakes",
}


def _ipc_prefix(code):
    """Extract the prefix used as IPC_DESCRIPTIONS key (e.g. H02P21/22 → H02P21)."""
    return code.split("/")[0] if code else ""


def name_sub_cluster(sub_summary, cluster_id):
    """Human-readable name for a sub-cluster. Manually-assigned names (from the
    cluster's top-10 central patents) take priority; falls back to a procedural
    name from top IPC codes when no manual name is set."""
    if cluster_id in SUB_NAMES:
        return SUB_NAMES[cluster_id]

    top_ipcs = sub_summary.get("top_ipc_groups") or []
    if not top_ipcs:
        return f"Sub-cluster {cluster_id}"

    primary_code = top_ipcs[0][0]
    primary_prefix = _ipc_prefix(primary_code)
    primary_desc = IPC_DESCRIPTIONS.get(primary_prefix, primary_prefix)

    secondary = None
    for code, _ in top_ipcs[1:4]:
        prefix = _ipc_prefix(code)
        if prefix != primary_prefix and prefix in IPC_DESCRIPTIONS:
            secondary = IPC_DESCRIPTIONS[prefix]
            break

    if secondary and secondary != primary_desc:
        return f"{primary_desc} + {secondary}"
    return f"{primary_desc} ({primary_code})"


def load_data():
    with open(OUT_DIR / "scope_metadata.json") as f:
        reps = json.load(f)
    with open(OUT_DIR / "clusters.json") as f:
        clusters = json.load(f)
    with open(OUT_DIR / f"cluster_summary_{MEGA_LEVEL}.json") as f:
        mega_summaries = json.load(f)
    with open(OUT_DIR / f"cluster_summary_{SUB_LEVEL}.json") as f:
        sub_summaries = json.load(f)
    return reps, clusters, mega_summaries, sub_summaries


def build_hierarchy(reps, clusters):
    """For each sub-cluster, assign its dominant mega-cluster parent (majority vote).
    Also returns exact per-(sub,mega) patent counts so plots can attribute cross-cut
    patents to the right parent — without this, parent/children sums don't match and
    Plotly treemaps/sunbursts silently fail to render."""
    sub_to_mega_votes = defaultdict(Counter)
    sub_size = Counter()
    mega_size = Counter()
    sub_mega_size = defaultdict(int)   # (sub, mega) -> count
    for r in reps:
        a = clusters[r["pub"]]
        mega = a[MEGA_LEVEL]
        sub = a[SUB_LEVEL]
        sub_to_mega_votes[sub][mega] += 1
        sub_size[sub] += 1
        mega_size[mega] += 1
        sub_mega_size[(sub, mega)] += 1
    sub_to_mega = {sub: votes.most_common(1)[0][0] for sub, votes in sub_to_mega_votes.items()}
    return sub_to_mega, sub_size, mega_size, sub_mega_size


def mega_label(mega_id, mega_size):
    name = MEGA_NAMES.get(mega_id, f"Mega-cluster {mega_id}")
    return f"M{mega_id}: {name} ({mega_size[mega_id]:,} patents)"


def short_sub_label(sub_id, sub_summary):
    """Build a human-readable short tag for a sub-cluster from its top IPC."""
    if not sub_summary or not sub_summary.get("top_ipc_groups"):
        return f"S{sub_id}"
    top_ipcs = [code for code, _ in sub_summary["top_ipc_groups"][:3]]
    return f"S{sub_id}: {' · '.join(top_ipcs)}"


def make_icicle_tree(mega_summaries, sub_summaries, sub_to_mega, sub_size,
                     mega_size, sub_mega_size):
    """Horizontal tree (Plotly Icicle): H02P → mega → named sub-clusters.
    Like an org chart: root on the left, branches expand right. Hover for detail."""
    sub_by_id = {c["cluster_id"]: c for c in sub_summaries}

    significant_megas = sorted(
        [m for m, s in mega_size.items() if s >= MIN_MEGA_SIZE],
        key=lambda m: -mega_size[m],
    )

    labels, parents, values, texts = ["H02P Motor Control"], [""], [sum(mega_size.values())], [""]

    for mega_id in significant_megas:
        mname = MEGA_NAMES.get(mega_id, f"Mega-cluster {mega_id}")
        mlabel = f"M{mega_id}: {mname}"
        labels.append(mlabel)
        parents.append("H02P Motor Control")
        values.append(mega_size[mega_id])
        texts.append(f"<b>{mega_size[mega_id]:,} patents</b>")

        owned_subs = [s for s, m in sub_to_mega.items() if m == mega_id]
        owned_large = [s for s in owned_subs
                       if sub_mega_size[(s, mega_id)] >= MIN_SUB_SIZE]
        owned_large.sort(key=lambda s: -sub_mega_size[(s, mega_id)])

        slice_total = 0
        for s in owned_large[:8]:
            ssum = sub_by_id.get(s, {})
            sname = name_sub_cluster(ssum, s)
            val = sub_mega_size[(s, mega_id)]
            labels.append(f"S{s}: {sname}")
            parents.append(mlabel)
            values.append(val)
            texts.append(
                f"<b>{val:,} patents in this mega</b><br>"
                f"Top IPC: {ssum.get('top_ipc_groups', [['—',0]])[0][0]}<br>"
                f"Top assignee: {ssum.get('top_assignees', [['—',0]])[0][0]}"
            )
            slice_total += val

        remainder = mega_size[mega_id] - slice_total
        if remainder > 0:
            labels.append(f"Other (M{mega_id})")
            parents.append(mlabel)
            values.append(remainder)
            texts.append(f"<b>{remainder:,} patents</b><br>"
                         f"<i>Small sub-clusters + cross-cuts</i>")

    other_mega_total = sum(s for m, s in mega_size.items() if s < MIN_MEGA_SIZE)
    if other_mega_total > 0:
        labels.append("Other specialty niches")
        parents.append("H02P Motor Control")
        values.append(other_mega_total)
        texts.append(f"<b>{other_mega_total:,} patents</b><br>"
                     f"<i>Aggregated smaller mega-clusters</i>")

    fig = go.Figure(
        go.Icicle(
            labels=labels,
            parents=parents,
            values=values,
            text=texts,
            hovertemplate="<b>%{label}</b><br>%{text}<extra></extra>",
            branchvalues="total",
            tiling=dict(orientation="h"),
            marker=dict(
                colors=values,
                colorscale=[(0, "#dbe9f7"), (0.5, "#5b9bd5"), (1.0, "#1f4e79")],
            ),
        )
    )
    fig.update_layout(
        title=dict(
            text="H02P Taxonomy — Named Tree",
            x=0.5, xanchor="center", font=dict(size=18),
        ),
        margin=dict(t=60, l=10, r=10, b=10),
        height=900,
        font=dict(size=12),
    )
    return fig


def build_text_tree(mega_summaries, sub_summaries, sub_to_mega, sub_size,
                    mega_size, sub_mega_size):
    """Render the full taxonomy as a clean nested HTML list, like a folder browser.
    Easy to read, easy to copy/paste into a doc or email."""
    sub_by_id = {c["cluster_id"]: c for c in sub_summaries}
    significant_megas = sorted(
        [m for m, s in mega_size.items() if s >= MIN_MEGA_SIZE],
        key=lambda m: -mega_size[m],
    )

    total = sum(mega_size.values())
    out = [f'<ul class="tree-root">']
    out.append(f'<li><span class="tree-root-label">'
               f'<b>H02P — Motor Control</b> '
               f'<span class="tree-count">{total:,} patents</span>'
               f'</span><ul>')

    for mega_id in significant_megas:
        mname = MEGA_NAMES.get(mega_id, f"Mega-cluster {mega_id}")
        msize = mega_size[mega_id]
        out.append(f'<li><span class="tree-mega-label">'
                   f'<span class="tree-mega-id">M{mega_id}</span> '
                   f'<b>{escape(mname)}</b> '
                   f'<span class="tree-count">{msize:,} patents</span>'
                   f'</span><ul>')

        owned_subs = [s for s, m in sub_to_mega.items() if m == mega_id]
        owned_large = [s for s in owned_subs
                       if sub_mega_size[(s, mega_id)] >= MIN_SUB_SIZE]
        owned_large.sort(key=lambda s: -sub_mega_size[(s, mega_id)])

        slice_total = 0
        for s in owned_large:
            ssum = sub_by_id.get(s, {})
            sname = name_sub_cluster(ssum, s)
            val = sub_mega_size[(s, mega_id)]
            slice_total += val
            top_assg = ssum.get("top_assignees", [])
            assignee_hint = (
                f" · top: {escape(top_assg[0][0])}" if top_assg else ""
            )
            out.append(
                f'<li><span class="tree-sub-label">'
                f'<span class="tree-sub-id">S{s}</span> '
                f'{escape(sname)} '
                f'<span class="tree-count">{val:,}</span>'
                f'<span class="tree-meta">{assignee_hint}</span>'
                f'</span></li>'
            )

        remainder = msize - slice_total
        if remainder > 0:
            out.append(
                f'<li class="tree-other"><span class="tree-sub-label">'
                f'<i>Other small sub-clusters + cross-cuts</i> '
                f'<span class="tree-count">{remainder:,}</span>'
                f'</span></li>'
            )

        out.append('</ul></li>')

    other_mega_total = sum(s for m, s in mega_size.items() if s < MIN_MEGA_SIZE)
    if other_mega_total > 0:
        out.append(
            f'<li class="tree-other"><span class="tree-mega-label">'
            f'<i>Other specialty niches</i> '
            f'<span class="tree-count">{other_mega_total:,} patents</span>'
            f'</span></li>'
        )

    out.append('</ul></li></ul>')
    return "\n".join(out)


def make_sankey(mega_summaries, sub_summaries, sub_to_mega, sub_size, mega_size,
                sub_mega_size, top_ipcs=12, top_subs_per_mega=3):
    """IPC groups → Mega → Sub flow diagram.

    Flows are computed from cluster summaries' top IPC counts (precise to the
    extent that the relevant IPC is in each mega's top-10 list)."""
    mega_by_id = {c["cluster_id"]: c for c in mega_summaries}
    sub_by_id = {c["cluster_id"]: c for c in sub_summaries}

    # (1) IPC totals across all megas
    ipc_mega_flow = {}
    ipc_totals = Counter()
    for msum in mega_summaries:
        mid = msum["cluster_id"]
        if mega_size[mid] < MIN_MEGA_SIZE:
            continue
        for ipc, cnt in msum.get("top_ipc_groups", []):
            ipc_mega_flow[(ipc, mid)] = cnt
            ipc_totals[ipc] += cnt
    top_ipc_list = [ipc for ipc, _ in ipc_totals.most_common(top_ipcs)]

    # (2) Significant megas sorted by size
    significant_megas = sorted(
        [m for m, s in mega_size.items() if s >= MIN_MEGA_SIZE],
        key=lambda m: -mega_size[m],
    )

    # (3) Top sub-clusters per mega
    mega_to_subs = defaultdict(list)
    for sub, mega in sub_to_mega.items():
        mega_to_subs[mega].append(sub)

    chosen_subs_per_mega = {}
    for m in significant_megas:
        # Top subs owned by this mega, ranked by patents-IN-THIS-MEGA (not total size)
        subs = [s for s in mega_to_subs[m]
                if sub_mega_size[(s, m)] >= MIN_SUB_SIZE]
        subs.sort(key=lambda s: -sub_mega_size[(s, m)])
        chosen_subs_per_mega[m] = subs[:top_subs_per_mega]

    # (4) Build Sankey node list
    nodes, node_idx = [], {}
    node_colors = []

    IPC_COLOR = "#94a3b8"      # slate
    MEGA_COLORS = ["#1f4e79", "#2563eb", "#0891b2", "#059669", "#ca8a04",
                   "#9333ea", "#dc2626", "#475569"]
    SUB_COLOR = "#cbd5e1"

    for ipc in top_ipc_list:
        node_idx[("ipc", ipc)] = len(nodes)
        nodes.append(f"IPC {ipc}")
        node_colors.append(IPC_COLOR)

    mega_color_lookup = {}
    for i, m in enumerate(significant_megas):
        name = MEGA_NAMES.get(m, f"Mega-cluster {m}")
        short = name if len(name) <= 50 else name[:47] + "…"
        node_idx[("mega", m)] = len(nodes)
        nodes.append(f"M{m}: {short}")
        col = MEGA_COLORS[i % len(MEGA_COLORS)]
        mega_color_lookup[m] = col
        node_colors.append(col)

    for m in significant_megas:
        for s in chosen_subs_per_mega[m]:
            label = short_sub_label(s, sub_by_id.get(s, {}))
            node_idx[("sub", s)] = len(nodes)
            nodes.append(label)
            node_colors.append(SUB_COLOR)

    # (5) Build edges
    sources, targets, values, link_colors = [], [], [], []
    for (ipc, mid), cnt in ipc_mega_flow.items():
        if ipc not in top_ipc_list:
            continue
        sources.append(node_idx[("ipc", ipc)])
        targets.append(node_idx[("mega", mid)])
        values.append(cnt)
        link_colors.append("rgba(148, 163, 184, 0.35)")

    for m in significant_megas:
        for s in chosen_subs_per_mega[m]:
            sources.append(node_idx[("mega", m)])
            targets.append(node_idx[("sub", s)])
            values.append(sub_mega_size[(s, m)])
            base = mega_color_lookup[m]
            link_colors.append(_hex_to_rgba(base, 0.45))

    fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                node=dict(
                    label=nodes,
                    pad=18,
                    thickness=20,
                    color=node_colors,
                    line=dict(color="white", width=0.5),
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    color=link_colors,
                ),
            )
        ]
    )
    fig.update_layout(
        title=dict(
            text=f"Patent Flow: Top {len(top_ipc_list)} IPC Groups → Mega-Clusters → Top Sub-Clusters",
            x=0.5, xanchor="center", font=dict(size=17),
        ),
        margin=dict(t=60, l=10, r=10, b=10),
        height=820,
        font=dict(size=12),
    )
    return fig


def make_sunburst(reps, clusters, mega_summaries, sub_summaries, mega_size,
                  sub_to_mega, sub_size, sub_mega_size):
    """H02P → Mega → Sub circular hierarchy.
    sub_mega_size needed to correctly account for sub-clusters that cross mega
    boundaries (otherwise branchvalues='total' parent/children sums don't match)."""
    mega_by_id = {c["cluster_id"]: c for c in mega_summaries}
    sub_by_id = {c["cluster_id"]: c for c in sub_summaries}

    significant_megas = sorted(
        [m for m, s in mega_size.items() if s >= MIN_MEGA_SIZE],
        key=lambda m: -mega_size[m],
    )

    labels, parents, values = ["H02P"], [""], [sum(mega_size.values())]

    other_mega_total = sum(s for m, s in mega_size.items() if s < MIN_MEGA_SIZE)

    for mega_id in significant_megas:
        mname = MEGA_NAMES.get(mega_id, f"Mega-cluster {mega_id}")
        short = mname if len(mname) <= 45 else mname[:42] + "…"
        mlabel = f"M{mega_id}: {short}"
        labels.append(mlabel)
        parents.append("H02P")
        values.append(mega_size[mega_id])

        # Find subs OWNED by this mega (majority vote) and pick top-8 by patents-in-mega.
        # Slice value uses exact (sub, mega) count, not total sub size — this preserves
        # branchvalues='total' accounting when subs cross mega boundaries.
        owned_subs = [s for s, m in sub_to_mega.items() if m == mega_id]
        owned_large = [s for s in owned_subs
                       if sub_mega_size[(s, mega_id)] >= MIN_SUB_SIZE]
        owned_large.sort(key=lambda s: -sub_mega_size[(s, mega_id)])
        top_n = owned_large[:8]

        slice_total = 0
        for s in top_n:
            ssum = sub_by_id.get(s, {})
            label = short_sub_label(s, ssum)
            labels.append(label)
            parents.append(mlabel)
            val = sub_mega_size[(s, mega_id)]
            values.append(val)
            slice_total += val

        # Remainder = all patents in this mega not yet attributed (small subs +
        # cross-cut patents from subs owned by other megas).
        remainder = mega_size[mega_id] - slice_total
        if remainder > 0:
            labels.append(f"Other in M{mega_id}")
            parents.append(mlabel)
            values.append(remainder)

    if other_mega_total > 0:
        labels.append("Other specialty niches")
        parents.append("H02P")
        values.append(other_mega_total)

    fig = go.Figure(
        go.Sunburst(
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            insidetextorientation="radial",
            marker=dict(
                colors=values,
                colorscale=[(0, "#dbe9f7"), (0.5, "#5b9bd5"), (1.0, "#1f4e79")],
            ),
            hovertemplate="<b>%{label}</b><br>Patents: %{value:,}<extra></extra>",
        )
    )
    fig.update_layout(
        title=dict(
            text="H02P Hierarchy — Circular View",
            x=0.5, xanchor="center", font=dict(size=17),
        ),
        margin=dict(t=60, l=10, r=10, b=10),
        height=750,
    )
    return fig


def _hex_to_rgba(hex_color, alpha):
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r}, {g}, {b}, {alpha})"


def make_treemap(reps, clusters, mega_summaries, sub_summaries, mega_size,
                 sub_to_mega, sub_size, sub_mega_size):
    """Build a Plotly treemap: root → mega → sub. Hover shows top assignees/IPC.
    Uses sub_mega_size for exact (sub, mega) counts so cross-cut patents don't
    inflate child sums beyond their parent mega's true total."""
    mega_by_id = {c["cluster_id"]: c for c in mega_summaries}
    sub_by_id = {c["cluster_id"]: c for c in sub_summaries}

    # Decide which megas / subs to keep at top level
    significant_megas = [m for m, s in mega_size.items() if s >= MIN_MEGA_SIZE]
    other_mega_total = sum(s for m, s in mega_size.items() if s < MIN_MEGA_SIZE)

    labels, parents, values, texts = ["All H02P (US+WO)"], [""], [sum(mega_size.values())], [""]

    for mega_id in sorted(significant_megas, key=lambda m: -mega_size[m]):
        mlabel = mega_label(mega_id, mega_size)
        labels.append(mlabel)
        parents.append("All H02P (US+WO)")
        values.append(mega_size[mega_id])
        msum = mega_by_id.get(mega_id, {})
        texts.append(
            "<br>".join([
                f"<b>Patents:</b> {mega_size[mega_id]:,}",
                f"<b>Top IPCs:</b> "
                + ", ".join(f"{c} ({n})" for c, n in (msum.get("top_ipc_groups") or [])[:5]),
                f"<b>Top assignees:</b> "
                + ", ".join(f"{a} ({n})" for a, n in (msum.get("top_assignees") or [])[:5]),
            ])
        )

        # Subs owned by this mega (majority vote) with exact (sub, mega) counts
        owned_subs = [s for s, m in sub_to_mega.items() if m == mega_id]
        owned_large = [s for s in owned_subs
                       if sub_mega_size[(s, mega_id)] >= MIN_SUB_SIZE]
        owned_large.sort(key=lambda s: -sub_mega_size[(s, mega_id)])

        slice_total = 0
        for sub_id in owned_large:
            ssum = sub_by_id.get(sub_id, {})
            val = sub_mega_size[(sub_id, mega_id)]
            labels.append(short_sub_label(sub_id, ssum))
            parents.append(mlabel)
            values.append(val)
            slice_total += val
            texts.append(
                "<br>".join([
                    f"<b>Patents in this mega:</b> {val:,}",
                    f"<b>Total cluster size:</b> {sub_size[sub_id]:,}",
                    f"<b>Top IPCs:</b> "
                    + ", ".join(f"{c} ({n})" for c, n in ssum.get("top_ipc_groups", [])[:5]),
                    f"<b>Top assignees:</b> "
                    + ", ".join(f"{a} ({n})" for a, n in ssum.get("top_assignees", [])[:5]),
                    "<b>Sample title:</b> "
                    + escape((ssum.get("sample_titles") or ["—"])[0] or "—")[:120],
                ])
            )

        remainder = mega_size[mega_id] - slice_total
        if remainder > 0:
            labels.append(f"Other in M{mega_id}")
            parents.append(mlabel)
            values.append(remainder)
            texts.append(f"<b>Patents:</b> {remainder:,}<br>"
                         f"<i>Small sub-clusters and cross-cut patents not shown above.</i>")

    if other_mega_total > 0:
        labels.append("Other specialty niches")
        parents.append("All H02P (US+WO)")
        values.append(other_mega_total)
        texts.append(f"<b>Patents:</b> {other_mega_total:,}<br>"
                     f"<i>Aggregated tail of mega-clusters with fewer than {MIN_MEGA_SIZE} patents.</i>")

    fig = go.Figure(
        go.Treemap(
            labels=labels,
            parents=parents,
            values=values,
            text=texts,
            hovertemplate="<b>%{label}</b><br>%{text}<extra></extra>",
            branchvalues="total",
            maxdepth=2,
            tiling=dict(packing="squarify"),
            marker=dict(
                colors=values,
                colorscale=[(0, "#dbe9f7"), (0.5, "#5b9bd5"), (1.0, "#1f4e79")],
                showscale=False,
            ),
        )
    )
    fig.update_layout(
        margin=dict(t=40, l=10, r=10, b=10),
        title=dict(
            text="H02P (US+WO) — Two-Level Citation-Based Taxonomy",
            x=0.5, xanchor="center",
            font=dict(size=18),
        ),
        height=750,
    )
    return fig


def mega_card_html(mega_id, mega_summaries, mega_size, sub_to_mega, sub_summaries):
    msum = next((c for c in mega_summaries if c["cluster_id"] == mega_id), {})
    name = MEGA_NAMES.get(mega_id, f"Mega-cluster {mega_id}")
    top_ipcs = msum.get("top_ipc_groups", [])[:6]
    top_assg = msum.get("top_assignees", [])[:6]
    samples = [t for t in (msum.get("sample_titles") or [])[:6] if t][:6]

    subs_here = sorted(
        [(s, sub_summaries_by_id[s]) for s in sub_to_mega
         if sub_to_mega[s] == mega_id and s in sub_summaries_by_id
         and sub_summaries_by_id[s]["size"] >= MIN_SUB_SIZE],
        key=lambda x: -x[1]["size"],
    )

    sub_rows = "\n".join(
        f"<tr><td>S{sid}</td><td style='text-align:right'>{ssum['size']:,}</td>"
        f"<td>{escape(', '.join(c for c,_ in ssum.get('top_ipc_groups', [])[:3]))}</td>"
        f"<td>{escape(', '.join(a for a,_ in ssum.get('top_assignees', [])[:3]))}</td></tr>"
        for sid, ssum in subs_here[:12]
    )

    return f"""
    <section class="card">
        <h3>M{mega_id} — {escape(name)}</h3>
        <div class="meta">
            <span class="pill">{mega_size[mega_id]:,} patents</span>
            <span class="pill">{len(subs_here)} sub-clusters (≥{MIN_SUB_SIZE} patents)</span>
        </div>
        <div class="cols">
            <div>
                <h4>Top IPC groups</h4>
                <ul>{''.join(f'<li><code>{escape(c)}</code> — {n:,}</li>' for c, n in top_ipcs)}</ul>
            </div>
            <div>
                <h4>Top assignees</h4>
                <ul>{''.join(f'<li>{escape(a)} — {n:,}</li>' for a, n in top_assg)}</ul>
            </div>
        </div>
        <h4>Sample titles</h4>
        <ul class="samples">{''.join(f'<li>{escape(t)[:160]}</li>' for t in samples)}</ul>
        <h4>Sub-clusters in this mega (top {min(12, len(subs_here))})</h4>
        <table class="subs">
            <thead><tr><th>ID</th><th>Size</th><th>Top IPC groups</th><th>Top assignees</th></tr></thead>
            <tbody>{sub_rows}</tbody>
        </table>
    </section>
    """


def build_html(icicle_fig, text_tree_html, sankey_fig, sunburst_fig, treemap_fig,
               reps, mega_summaries, sub_summaries, mega_size, sub_to_mega):
    global sub_summaries_by_id
    sub_summaries_by_id = {c["cluster_id"]: c for c in sub_summaries}

    # Inline Plotly JS once (in the FIRST figure); subsequent figures reuse the loaded JS.
    icicle_html = icicle_fig.to_html(full_html=False, include_plotlyjs="inline")
    sankey_html = sankey_fig.to_html(full_html=False, include_plotlyjs=False)
    sunburst_html = sunburst_fig.to_html(full_html=False, include_plotlyjs=False)
    treemap_html = treemap_fig.to_html(full_html=False, include_plotlyjs=False)

    n_total = len(reps)
    clustered_count = sum(1 for s in sub_size_global.values() if s >= 2) if False else 0
    # Use the cluster sizes from sub_size_global computed earlier
    significant_megas = sorted(
        [(m, s) for m, s in mega_size.items() if s >= MIN_MEGA_SIZE],
        key=lambda x: -x[1],
    )
    cards_html = "\n".join(
        mega_card_html(mega_id, mega_summaries, mega_size, sub_to_mega, sub_summaries)
        for mega_id, _ in significant_megas
    )

    # Headline numbers
    isolated = sum(1 for s in sub_size_global.values() if s == 1)
    in_clusters = n_total - isolated

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>H02P Patent Taxonomy — {CLUSTER_VERSION}</title>
<style>
  :root {{
    --fg:#1f2937; --muted:#6b7280; --bg:#f8fafc; --card:#ffffff;
    --accent:#1f4e79; --accent-light:#5b9bd5; --border:#e5e7eb;
  }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         max-width: 1200px; margin: 0 auto; padding: 32px 24px;
         color: var(--fg); background: var(--bg); line-height: 1.55; }}
  h1 {{ font-size: 28px; margin: 0 0 8px 0; color: var(--accent); }}
  h2 {{ font-size: 20px; margin: 32px 0 12px; color: var(--accent); border-bottom: 1px solid var(--border); padding-bottom: 6px; }}
  h3 {{ font-size: 17px; margin: 0 0 6px; color: var(--accent); }}
  h4 {{ font-size: 13px; text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted); margin: 12px 0 6px; }}
  .lede {{ color: var(--muted); margin-bottom: 24px; }}
  .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin: 16px 0 32px; }}
  .stat {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }}
  .stat .num {{ font-size: 22px; font-weight: 600; color: var(--accent); }}
  .stat .lbl {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }}
  .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px;
           padding: 20px 24px; margin: 20px 0; box-shadow: 0 1px 2px rgba(0,0,0,0.03); }}
  .meta {{ margin: 6px 0 14px; }}
  .pill {{ display: inline-block; background: var(--accent-light); color: white;
           border-radius: 999px; padding: 3px 10px; font-size: 12px; margin-right: 6px; }}
  .cols {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
  ul {{ margin: 4px 0 8px; padding-left: 20px; }}
  ul.samples li {{ font-size: 13px; color: var(--muted); }}
  code {{ background: #eef2f7; padding: 1px 4px; border-radius: 3px; font-size: 12px; }}
  table.subs {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-top: 6px; }}
  table.subs th, table.subs td {{ border-bottom: 1px solid var(--border); padding: 6px 8px; text-align: left; }}
  table.subs th {{ background: #f1f5f9; color: var(--muted); font-weight: 600; text-transform: uppercase; font-size: 11px; letter-spacing: 0.04em; }}
  footer {{ color: var(--muted); font-size: 12px; margin-top: 40px; border-top: 1px solid var(--border); padding-top: 14px; }}
  .caveat {{ background: #fff7ed; border: 1px solid #fdba74; border-radius: 8px; padding: 12px 16px; margin: 16px 0; font-size: 14px; }}
  /* Folder-style text tree */
  ul.tree-root, ul.tree-root ul {{ list-style: none; padding-left: 0; margin: 4px 0; }}
  ul.tree-root ul {{ padding-left: 22px; margin-left: 8px; border-left: 1.5px dashed #cbd5e1; }}
  ul.tree-root li {{ position: relative; padding: 4px 0 4px 18px; }}
  ul.tree-root li::before {{ content: "├─"; position: absolute; left: 0; top: 8px; color: #94a3b8; font-family: monospace; font-size: 13px; }}
  ul.tree-root > li::before {{ content: ""; }}
  ul.tree-root > li {{ padding-left: 0; }}
  .tree-root-label {{ display: inline-block; padding: 4px 12px; background: var(--accent); color: white; border-radius: 6px; font-size: 15px; }}
  .tree-mega-label {{ display: inline-block; font-size: 14px; }}
  .tree-mega-id {{ display: inline-block; background: var(--accent-light); color: white; padding: 1px 8px; border-radius: 4px; font-family: monospace; font-size: 12px; margin-right: 8px; }}
  .tree-sub-label {{ font-size: 13px; color: var(--fg); }}
  .tree-sub-id {{ display: inline-block; background: #e2e8f0; color: var(--accent); padding: 1px 6px; border-radius: 3px; font-family: monospace; font-size: 11px; margin-right: 8px; }}
  .tree-count {{ display: inline-block; background: #f1f5f9; color: var(--muted); padding: 1px 7px; border-radius: 10px; font-size: 11px; margin-left: 6px; font-variant-numeric: tabular-nums; }}
  .tree-meta {{ color: var(--muted); font-size: 11px; margin-left: 6px; font-style: italic; }}
  .tree-other {{ opacity: 0.65; }}
  details.extra-charts {{ margin-top: 32px; }}
  details.extra-charts summary {{ cursor: pointer; padding: 10px 14px; background: var(--card); border: 1px solid var(--border); border-radius: 8px; font-weight: 600; color: var(--accent); }}
  details.extra-charts[open] summary {{ margin-bottom: 12px; }}
</style>
</head>
<body>

<h1>H02P Patent Taxonomy — {CLUSTER_VERSION}</h1>
<p class="lede">{'Citation + title + abstract similarity' if CLUSTER_VERSION == 'v3' else ('Citation + title-similarity' if CLUSTER_VERSION == 'v2' else 'Citation-based')} community detection on US + WO motor-control patents (IPC subclass H02P), producing a two-level alternative to IPC classification. {f'<b>{CLUSTER_VERSION}</b> combines CITES_PATENT (weight 3.0), SIMILAR_TITLE (score×0.4), and SIMILAR_ABSTRACT (score×0.6) edges.' if CLUSTER_VERSION == 'v3' else (f'<b>{CLUSTER_VERSION}</b> uses CITES_PATENT edges weighted 3.0 plus SIMILAR_TITLE edges weighted score×0.5.' if CLUSTER_VERSION == 'v2' else f'<b>{CLUSTER_VERSION}</b> uses CITES_PATENT edges only.')}</p>

<div class="stats">
  <div class="stat"><div class="num">{n_total:,}</div><div class="lbl">Family representatives</div></div>
  <div class="stat"><div class="num">{in_clusters:,}</div><div class="lbl">Clustered (72%)</div></div>
  <div class="stat"><div class="num">{len(significant_megas)}</div><div class="lbl">Mega-categories</div></div>
  <div class="stat"><div class="num">0.66</div><div class="lbl">Modularity Q</div></div>
</div>

<h2>The Taxonomy as a Tree</h2>
<p>Root: <b>H02P (US + WO Motor Control)</b>. Branches: <b>5 mega-categories</b>. Leaves: <b>~25 named sub-clusters</b>. Sub-cluster names are derived from each cluster's dominant IPC group description (see methodology). Hover any segment in the chart below or read the named tree underneath.</p>

{icicle_html}

<h3>The Same Tree, in Text</h3>
<p>Readable, printable, copy-pasteable. Numbers in pills show patent counts <i>within this mega</i> (cross-cut patents in a sub-cluster are attributed to the mega they actually belong to).</p>

<div class="card">
{text_tree_html}
</div>

<details class="extra-charts">
<summary>▸ Additional views (Sankey, Sunburst, Treemap)</summary>

<h3>IPC → Mega → Sub (Sankey flow)</h3>
<p>How top H02P IPC groups distribute across our mega-clusters. Shows the "cross-cuts" — how our taxonomy reorganizes IPC.</p>
{sankey_html}

<h3>Circular Hierarchy (Sunburst)</h3>
<p>Same hierarchy, radial layout. Click any segment to zoom in.</p>
{sunburst_html}

<h3>Cluster Sizes (Treemap)</h3>
<p>Proportional rectangles. Click any tile to drill in. Hover for top IPCs, assignees, sample title.</p>
{treemap_html}

</details>

<h2>Mega-Categories in Detail</h2>
<p>Each mega-category is a coarse-resolution Leiden community (γ=0.3) that maps cleanly onto a known motor-control sub-discipline. Names are tentative — assigned from top IPC codes and sample titles, awaiting expert review.</p>
{cards_html}

<h2>How This Was Built</h2>
<ul>
    <li><b>Scope:</b> 89,402 patents in IPC H02P filed in US or WO (PCT), collapsed to {n_total:,} family representatives (earliest filing date per family).</li>
    <li><b>Graph:</b> Direct citation edges between scope patents → 130,096 undirected edges across 16,593 connected components. Largest component holds 38,322 patents.</li>
    <li><b>Algorithm:</b> Leiden community detection (<code>leidenalg.RBConfigurationVertexPartition</code>) at multiple resolutions:
        <ul>
            <li>γ=0.3 → <b>mega-categories</b> (this report)</li>
            <li>γ=1.0 → <b>sub-clusters</b> (this report)</li>
            <li>γ=0.5, 2.0 also computed for sensitivity</li>
        </ul>
    </li>
    <li><b>Hierarchy:</b> Each sub-cluster is mapped to its dominant mega-cluster (majority of member patents).</li>
    <li><b>Quality:</b> Modularity Q = 0.66 at γ=1.0 (Q &gt; 0.4 is the threshold for strong community structure); robust across γ=0.5–2.0.</li>
</ul>

<h2>Known Limitations</h2>
<div class="caveat">
    <b>15,783 patents (28% of scope) are unclustered</b> — they have no in-scope citation edges and currently sit as singletons. To assign them, we'll either compute title+abstract embeddings (PatentSBERTa) or wait for SIMILAR_TITLE / SIMILAR_ABSTRACT edges to populate. Until then, this taxonomy covers ~72% of the corpus.
</div>
<div class="caveat">
    <b>US + WO scope only.</b> Citation ingestion is currently incomplete for JP, EP, KR, CN, and most smaller jurisdictions (49% of H02P globally has zero outgoing citations vs. 13.5% for US+WO). The cluster <i>structure</i> should generalize to the global corpus, but cluster <i>membership</i> for non-US/WO patents requires a fallback view (embeddings or text similarity).
</div>
<div class="caveat">
    <b>Cluster names are tentative.</b> The mega-category names in this report are assigned from top IPC distributions and sample titles. They should be reviewed by a domain expert and may be refined.
</div>
<div class="caveat">
    <b>OCR garbage in older titles.</b> Some pre-2000 US titles ("h rosenthal", "Asbionob to westinohotrse") are corrupted in the source data. This affects display only, not clustering — the citation graph itself is intact.
</div>

<footer>
    Generated from <code>cluster_h02p_v1_out/</code>. Methodology: Leiden community detection on the
    H02P citation subgraph, with two-level resolution (γ=0.3 mega, γ=1.0 sub).
    No mutations have been written to the knowledge graph; this report describes a candidate
    classification awaiting review.
</footer>

</body>
</html>
"""


def main():
    reps, clusters, mega_summaries, sub_summaries = load_data()
    sub_to_mega, sub_size, mega_size, sub_mega_size = build_hierarchy(reps, clusters)
    global sub_size_global
    sub_size_global = sub_size

    print(f"Loaded {len(reps):,} reps, {len(mega_summaries)} mega-summaries, "
          f"{len(sub_summaries)} sub-summaries")
    print(f"Mega-categories with ≥{MIN_MEGA_SIZE} patents: "
          f"{sum(1 for s in mega_size.values() if s >= MIN_MEGA_SIZE)}")
    print(f"Sub-clusters with ≥{MIN_SUB_SIZE} patents: "
          f"{sum(1 for s in sub_size.values() if s >= MIN_SUB_SIZE)}")

    print("Building figures...")
    icicle_fig = make_icicle_tree(mega_summaries, sub_summaries, sub_to_mega,
                                  sub_size, mega_size, sub_mega_size)
    text_tree_html = build_text_tree(mega_summaries, sub_summaries, sub_to_mega,
                                     sub_size, mega_size, sub_mega_size)
    sankey_fig = make_sankey(mega_summaries, sub_summaries, sub_to_mega, sub_size,
                             mega_size, sub_mega_size)
    sunburst_fig = make_sunburst(reps, clusters, mega_summaries, sub_summaries,
                                 mega_size, sub_to_mega, sub_size, sub_mega_size)
    treemap_fig = make_treemap(reps, clusters, mega_summaries, sub_summaries,
                               mega_size, sub_to_mega, sub_size, sub_mega_size)

    print("Building HTML (inlining Plotly JS for full portability)...")
    html = build_html(icicle_fig, text_tree_html, sankey_fig, sunburst_fig, treemap_fig,
                     reps, mega_summaries, sub_summaries, mega_size, sub_to_mega)

    out_path = OUT_DIR / f"hierarchy_report_{CLUSTER_VERSION}.html"
    out_path.write_text(html)
    print(f"\nReport written: {out_path}")
    print(f"  Open in browser: file://{out_path}")
    print(f"  Size: {out_path.stat().st_size / (1024*1024):.2f} MB (self-contained, no CDN needed)")


if __name__ == "__main__":
    main()
