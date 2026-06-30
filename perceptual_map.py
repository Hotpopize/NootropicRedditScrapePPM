"""Perceptual map generator for the submissions-only stratum.

Plots the six Reddit communities on a push-pull / mooring-intensity plane.

H-1  Count-derived axes:
        X = (PULL - PUSH) / (PULL + PUSH)   push-pull balance, bounded [-1, 1]
        Y = 100 * present / n               mooring-intensity presence rate
H-2  Empty-quadrant suppression: quadrant captions are drawn only where points
        fall. All six communities sit in the pull-dominant right half, so the two
        left-hand quadrants are suppressed.
H-3  Archetype recolour: the facilitated/inhibited direction marker channel is
        retired. Every community is plotted with a single circle marker, coloured
        by its locked consumer archetype. All six communities are net-facilitated
        (MOOR-F exceeds MOOR-I in every community), so the direction channel
        carried no information.

The production render requires a frozen JSON input. There is no dummy-data
fallback.
"""

import argparse
import json
import os
import sys
from statistics import median

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter


# --- Locked typology -------------------------------------------------------
# Three archetypes, not five. There are no inhibitor archetypes; MOOR-I is
# structurally under-coded. Casing is load-bearing: NooTopics (n=87) is
# Facilitator-Embedded, Nootropics (n=105) is Self-Directed. Matching is
# case-sensitive on the bare community name to keep the two apart.

ARCHETYPES = {
    "Facilitator-Embedded Wellness Seeker": {
        "communities": {"NooTopics", "Supplements"},
        "fill": "#2E8B57",
        "edge": "#1B5E3B",
    },
    "Self-Directed Natural Optimiser": {
        "communities": {"Biohackers", "Nootropics", "StackAdvice"},
        "fill": "#2C6FB5",
        "edge": "#1A4576",
    },
    "Caffeine/Stimulant Escapee": {
        "communities": {"Decaf"},
        "fill": "#C0552B",
        "edge": "#7A3318",
    },
}


def _bare_name(community):
    """Strip an optional r/ prefix. Casing is preserved on purpose."""
    return community[2:] if community.startswith("r/") else community


def archetype_for(community):
    """Return the archetype name for a community, case-sensitive."""
    bare = _bare_name(community)
    for name, spec in ARCHETYPES.items():
        if bare in spec["communities"]:
            return name
    raise ValueError(
        "Community %r maps to no locked archetype. Check casing "
        "(NooTopics vs Nootropics) and the frozen JSON." % community
    )


def load_frozen_json(path):
    """Load the frozen stratum slice and derive H-1 coordinates.

    Returns:
        points: list of point dicts: community, n, x, y, archetype, fill, edge.
        stratum: string, stratum name.
        n_total: int, total units in stratum.
    """
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    stratum = payload.get("_stratum", "submissions_only")
    n_total = payload.get("_n_total", 609)

    # Filter out metadata keys like _stratum, _n_total, etc.
    communities = {k: v for k, v in payload.items() if not k.startswith("_")}
    points = []
    for community, vals in sorted(communities.items()):
        # Y-axis divisor safeguard: resolve divisor strictly based on stratum key
        if stratum == "full_corpus":
            n = vals["total_units"]
        else:
            n = vals["submissions_n"]
        push = vals["PUSH_count"]
        pull = vals["PULL_count"]
        present = vals["MOOR_present_count"]

        x = (pull - push) / (pull + push)   # H-1
        y = 100.0 * present / n             # H-1

        arch = archetype_for(community)
        spec = ARCHETYPES[arch]
        points.append(
            {
                "community": community,
                "n": n,
                "push": push,
                "pull": pull,
                "present": present,
                "x": x,
                "y": y,
                "archetype": arch,
                "fill": spec["fill"],
                "edge": spec["edge"],
            }
        )
    return points, stratum, n_total


def print_audit_trail(points):
    """Per-community audit trail reproducing the certified coordinates."""
    header = "{:<12} {:>4} {:>5} {:>5} {:>8} {:>9} {:>9}  {}".format(
        "community", "n", "PUSH", "PULL", "present", "X", "Y", "archetype"
    )
    print(header)
    print("-" * len(header))
    for p in sorted(points, key=lambda d: d["x"], reverse=True):
        print(
            "{:<12} {:>4} {:>5} {:>5} {:>8} {:>9.3f} {:>9.3f}  {}".format(
                p["community"], p["n"], p["push"], p["pull"], p["present"],
                p["x"], p["y"], p["archetype"],
            )
        )


def render(points, svg_path, png_path, color_archetypes=False, stratum="submissions_only", n_total=609):
    """Render the perceptual map to SVG and PNG."""
    fig, ax = plt.subplots(figsize=(9.0, 6.5))

    xs = [p["x"] for p in points]
    ys = [p["y"] for p in points]

    # Reference lines. Vertical at X=0 splits push-dominant from pull-dominant.
    # Horizontal at the Y median splits lower from higher mooring prevalence.
    y_divider = median(ys)
    ax.axvline(0.0, color="#999999", linewidth=0.8, zorder=1)
    ax.axhline(y_divider, color="#999999", linewidth=0.8, linestyle="--", zorder=1)

    stratum_display = "submissions stratum" if stratum == "submissions_only" else "full-corpus stratum"
    n_display = f"{n_total:,}"

    if color_archetypes:
        x_lo, x_hi = -1.0, 1.0
        y_lo = 0.0
        y_hi = max(ys) * 1.18
        ax.set_xlim(x_lo, x_hi)
        ax.set_ylim(y_lo, y_hi)

        # H-2 empty-quadrant suppression. Cap captions to occupied regions only.
        occupied_right = any(p["x"] > 0 for p in points)
        occupied_left = any(p["x"] <= 0 for p in points)
        quadrant_captions = []
        if occupied_right:
            quadrant_captions.append(
                (0.5 * x_hi, y_hi - 0.04 * (y_hi - y_lo),
                 "pull-dominant, higher mooring intensity")
            )
            quadrant_captions.append(
                (0.5 * x_hi, y_lo + 0.03 * (y_hi - y_lo),
                 "pull-dominant, lower mooring intensity")
            )
        if occupied_left:
            quadrant_captions.append(
                (0.5 * x_lo, y_hi - 0.04 * (y_hi - y_lo),
                 "push-dominant, higher mooring intensity")
            )
            quadrant_captions.append(
                (0.5 * x_lo, y_lo + 0.03 * (y_hi - y_lo),
                 "push-dominant, lower mooring intensity")
            )

        for cx, cy, text in quadrant_captions:
            ax.text(cx, cy, text, ha="center", va="center", fontsize=8,
                    color="#B0B0B0", style="italic", zorder=1)

        # One circle marker for all communities; colour encodes archetype.
        for p in points:
            ax.scatter(
                p["x"], p["y"], marker="o", s=190,
                facecolor=p["fill"], edgecolor=p["edge"], linewidth=1.6, zorder=3,
            )
            # Position labels to avoid overlaps
            bare = _bare_name(p["community"])
            xytext = (-32, -13) if bare == "Nootropics" else (9, 3) if bare == "Biohackers" else (9, 6)
            ax.annotate(
                "r/" + bare,
                (p["x"], p["y"]), textcoords="offset points", xytext=xytext,
                fontsize=9, color="#222222", zorder=4,
            )

        ax.set_xlabel("Push-pull balance  (PULL - PUSH) / (PULL + PUSH)", fontsize=10)
        ax.set_ylabel("Mooring intensity  (100 × present / n)", fontsize=10)
        ax.set_title(
            f"Consumer archetypes across six nootropics communities ({stratum_display}, N={n_display})",
            fontsize=11,
        )

        # Legend lists the three locked archetypes and reference lines.
        handles = [
            Line2D([0], [0], marker="o", linestyle="none", markersize=11,
                   markerfacecolor=spec["fill"], markeredgecolor=spec["edge"],
                   markeredgewidth=1.6, label=name)
            for name, spec in ARCHETYPES.items()
        ]
        handles.append(
            Line2D([0], [0], color="#999999", linestyle="--", linewidth=1.0,
                   label=f"Median Mooring Intensity Divider ({y_divider:.1f}%)")
        )
        handles.append(
            Line2D([0], [0], color="#999999", linestyle="-", linewidth=0.8,
                   label="Push-Pull Equilibrium (X = 0.0)")
        )
        ax.legend(handles=handles, loc="upper left", fontsize=8.5,
                  title="Consumer archetype / Reference lines", title_fontsize=9, framealpha=0.95)

        # Methodological footnote.
        footnote = (
            "All six communities are net-facilitated (MOOR-F exceeds MOOR-I in every "
            "community). The facilitated/inhibited direction marker channel therefore "
            "carried no information and has been retired; marker colour now encodes the "
            "locked consumer archetype. Axes are count-derived (H-1); captions for the "
            "empty push-dominant quadrants are suppressed if they contain no points (H-2)."
        )
        fig.text(0.01, 0.005, footnote, ha="left", va="bottom", fontsize=6.8,
                 color="#555555", wrap=True)

        fig.subplots_adjust(left=0.09, right=0.97, top=0.93, bottom=0.16)

    else:
        # Neutral map showing the points on the map without coloring by archetype.
        # Matches user's exact uploaded design.
        ax.set_xlim(-1.0, 1.0)
        ax.set_ylim(0.0, 35.0)
        ax.set_xticks([-1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0])
        ax.set_yticks([0, 5, 10, 15, 20, 25, 30, 35])
        
        ax.xaxis.set_major_formatter(FuncFormatter(lambda val, _: f"{val:.2f}"))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda val, _: f"{int(val)}"))
        
        # Grid lines: light gray dotted grid
        ax.grid(True, linestyle=":", alpha=0.5, color="#E0E0E0", zorder=0)
        ax.set_axisbelow(True)

        # Plot all communities as neutral dark gray circles
        for p in points:
            ax.scatter(
                p["x"], p["y"], marker="o", s=190,
                facecolor="#4B4B4B", edgecolor="#222222", linewidth=1.6, zorder=3,
            )
            
            # Position labels to avoid overlaps, matching the user's image exactly
            bare = _bare_name(p["community"])
            if bare == "Nootropics":
                xytext = (-32, -13)
            elif bare == "Biohackers":
                xytext = (9, 1)
            else:
                xytext = (9, 3)
                
            ax.annotate(
                "r/" + bare,
                (p["x"], p["y"]), textcoords="offset points", xytext=xytext,
                fontsize=9, color="#222222", zorder=4,
            )

        # Math minus sign '\u2212' matches user's exact label
        ax.set_xlabel("Push-pull balance  (PULL \u2212 PUSH) / (PULL + PUSH)", fontsize=10)
        ax.set_ylabel("Mooring prevalence  (100 \u00d7 present / n)", fontsize=10)
        ax.set_title(
            f"Six nootropics communities: push-pull balance against mooring prevalence\n({stratum_display}, N = {n_display})",
            fontsize=11,
        )

        # Legend lists only reference lines
        handles = [
            Line2D([0], [0], color="#999999", linestyle="--", linewidth=1.0,
                   label=f"Median mooring prevalence ({y_divider:.1f}%)"),
            Line2D([0], [0], color="#999999", linestyle="-", linewidth=0.8,
                   label="Push-pull equilibrium (X = 0.0)")
        ]
        ax.legend(handles=handles, loc="upper left", fontsize=9.5, framealpha=0.95)

        # Clean margin adjustment without footnote
        fig.subplots_adjust(left=0.09, right=0.97, top=0.92, bottom=0.11)

    fig.savefig(svg_path, format="svg")
    fig.savefig(png_path, format="png", dpi=200)
    plt.close(fig)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Render the submissions-only perceptual map.")
    parser.add_argument("--input", help="Path to the frozen submissions JSON.")
    parser.add_argument("--output", help="Output SVG path.", required=True)
    parser.add_argument("--color-archetypes", action="store_true", help="Color points by locked consumer archetype.")
    args = parser.parse_args(argv)

    # Production render requires a frozen JSON input. No dummy-data fallback.
    if not args.input:
        sys.exit("error: --input is required (frozen submissions JSON). No dummy-data fallback.")
    if not args.input.lower().endswith(".json"):
        sys.exit("error: --input must be a .json file: %s" % args.input)
    if not os.path.isfile(args.input):
        sys.exit("error: --input file does not exist: %s" % args.input)

    points, stratum, n_total = load_frozen_json(args.input)

    svg_path = args.output
    if not svg_path.lower().endswith(".svg"):
        svg_path = svg_path + ".svg"
    png_path = os.path.splitext(svg_path)[0] + ".png"

    os.makedirs(os.path.dirname(svg_path) or ".", exist_ok=True)

    print_audit_trail(points)

    occupied_left = any(p["x"] <= 0 for p in points)
    left_suppressed = 2 if not occupied_left else 0

    if args.color_archetypes:
        n_colors = len({p["fill"] for p in points})
        print("\nmarker colours rendered: %d" % n_colors)
        print(f"left quadrants suppressed: {left_suppressed} ({'no push-dominant points' if not occupied_left else 'push-dominant points present'})")
    else:
        print("\nmarker colours rendered: 1 (neutral mode)")
        print(f"left quadrants suppressed: {left_suppressed} ({'no push-dominant points' if not occupied_left else 'push-dominant points present'})")

    render(points, svg_path, png_path, color_archetypes=args.color_archetypes, stratum=stratum, n_total=n_total)
    print("\nwrote %s" % svg_path)
    print("wrote %s" % png_path)


if __name__ == "__main__":
    main()
