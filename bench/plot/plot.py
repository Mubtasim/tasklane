"""Charts for docs/benchmarks.md from a bench results directory.

Usage: plot.py <results dir> <output dir> [--only NAME,...] [--before DIR]

Only valid rows are plotted; docs/benchmarks.md lists any invalid runs.

--only draws just the named charts (file names without .png), so charts can
come from different result directories: each chart from the directory that
holds its data. --before adds a second, dashed curve to the latency chart
from an earlier results directory, for a before/after comparison.

The latency chart uses the "open<pct>-w<slots>-medianM-<n>" rows (open runs
at a share of the median closed maximum) when the directory has them, and
the suite's "s2-open<pct>-w<slots>" rows otherwise.
"""
import argparse
import csv
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

INK, ACCENT, MUTED = "#1f2937", "#2563eb", "#9ca3af"
plt.rcParams.update({"figure.figsize": (6.4, 3.6), "figure.dpi": 150, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.edgecolor": MUTED, "axes.labelcolor": INK,
                     "xtick.color": INK, "ytick.color": INK, "font.size": 9})

CHARTS = ("s1-batch", "s2-throughput", "s3-payload", "s2-latency-vs-load")


def rows(results):
    with open(results / "results.csv", newline="") as f:
        return [r for r in csv.DictReader(f) if r["valid"] == "true"]


def by_label(rs, pattern):
    rx = re.compile(pattern)
    return [r for r in rs if rx.fullmatch(r["label"])]


def save(fig, out, name):
    fig.tight_layout()
    fig.savefig(out / name)
    plt.close(fig)


def bars(out, name, rs, x_key, x_fmt, title, xlabel):
    if not rs:
        return
    rs = sorted(rs, key=lambda r: float(r[x_key]))
    xs = [x_fmt(r[x_key]) for r in rs]
    ys = [float(r["throughput_jobs_s"]) for r in rs]
    fig, ax = plt.subplots()
    ax.bar(xs, ys, color=ACCENT)
    for i, y in enumerate(ys):
        ax.annotate(f"{y:,.0f}", (i, y), ha="center", va="bottom", fontsize=8, color=INK)
    ax.set(title=title, xlabel=xlabel, ylabel="jobs/s")
    save(fig, out, name)


def latency_rows(rs):
    median = by_label(rs, r"open\d+-w\d+-medianM-\d+")
    lat = median or by_label(rs, r"s2-open\d+-w\d+")
    return sorted(lat, key=lambda r: float(r["target_rate"]))


def latency(out, rs, before):
    lat = latency_rows(rs)
    if not lat:
        return
    fig, ax = plt.subplots()
    curves = [(lat, "", "-")]
    if before:
        curves = [(before, "before: ", "--"), (lat, "after: ", "-")]
    for data, prefix, style in curves:
        x = [float(r["target_rate"]) for r in data]
        for key, label, color in (("e2s_p50_ms", "p50", MUTED), ("e2s_p99_ms", "p99", ACCENT)):
            ax.plot(x, [float(r[key]) for r in data], marker="o", linestyle=style,
                    label=prefix + label, color=color)
    ax.set(title="Enqueue-to-start latency vs offered load (open)", xlabel="offered load, jobs/s",
           ylabel="ms")
    if before:
        ax.set_yscale("log")
    ax.legend(frameon=False)
    save(fig, out, "s2-latency-vs-load.png")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results", type=Path)
    ap.add_argument("out", type=Path)
    ap.add_argument("--only", default=",".join(CHARTS),
                    help="comma-separated chart names to draw (default: all)")
    ap.add_argument("--before", type=Path,
                    help="earlier results directory to compare in the latency chart")
    args = ap.parse_args()
    only = set(args.only.split(","))
    unknown = only - set(CHARTS)
    if unknown:
        ap.error(f"unknown chart(s): {', '.join(sorted(unknown))}; known: {', '.join(CHARTS)}")
    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    rs = rows(args.results)
    if "s1-batch" in only:
        bars(out, "s1-batch.png", by_label(rs, r"s1-closed-b\d+"), "batch", str,
             "Enqueue throughput by batch size (closed)", "jobs per call")
    if "s2-throughput" in only:
        bars(out, "s2-throughput.png", by_label(rs, r"s2-closed-w\d+"), "workers", str,
             "Full-cycle throughput by worker slots (closed)", "worker slots")
    if "s3-payload" in only:
        bars(out, "s3-payload.png", by_label(rs, r"s3-closed-p\d+"), "payload_bytes",
             lambda v: f"{int(v) // 1000} KB" if int(v) >= 1000 else f"{v} B",
             "Full-cycle throughput by payload size (closed)", "payload")
    if "s2-latency-vs-load" in only:
        before = latency_rows(rows(args.before)) if args.before else None
        latency(out, rs, before)


if __name__ == "__main__":
    main()
