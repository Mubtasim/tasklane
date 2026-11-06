"""Charts for docs/benchmarks.md from a bench results directory.

Usage: plot.py <results dir> <output dir>
Only valid rows are plotted; the report lists invalid ones separately.
"""
import csv
import re
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

INK, ACCENT, MUTED = "#1f2937", "#2563eb", "#9ca3af"
plt.rcParams.update({"figure.figsize": (6.4, 3.6), "figure.dpi": 150, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.edgecolor": MUTED, "axes.labelcolor": INK,
                     "xtick.color": INK, "ytick.color": INK, "font.size": 9})


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


def main():
    results, out = Path(sys.argv[1]), Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    rs = rows(results)
    bars(out, "s1-batch.png", by_label(rs, r"s1-closed-b\d+"), "batch", str,
         "Enqueue throughput by batch size (closed)", "jobs per call")
    bars(out, "s2-throughput.png", by_label(rs, r"s2-closed-w\d+"), "workers", str,
         "Full-cycle throughput by worker slots (closed)", "worker slots")
    bars(out, "s3-payload.png", by_label(rs, r"s3-closed-p\d+"), "payload_bytes",
         lambda v: f"{int(v) // 1000} KB" if int(v) >= 1000 else f"{v} B",
         "Full-cycle throughput by payload size (closed)", "payload")

    lat = sorted(by_label(rs, r"s2-open\d+-w\d+"), key=lambda r: float(r["target_rate"]))
    if lat:
        fig, ax = plt.subplots()
        x = [float(r["target_rate"]) for r in lat]
        for key, label, color in (("e2s_p50_ms", "p50", MUTED), ("e2s_p99_ms", "p99", ACCENT)):
            ax.plot(x, [float(r[key]) for r in lat], marker="o", label=label, color=color)
        ax.set(title="Enqueue-to-start latency vs offered load (open)", xlabel="offered load, jobs/s",
               ylabel="ms")
        ax.legend(frameon=False)
        save(fig, out, "s2-latency-vs-load.png")


if __name__ == "__main__":
    main()
