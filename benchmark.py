"""
Scalability benchmark: runs taxi_analysis.py with different numbers of cores.

One warm-up run is discarded and every configuration is repeated REPEATS times
keeping the median as the final result.

Usage:
    python benchmark.py
Outputs:
    benchmark_results.csv, speedup_plot.png, efficiency_plot.png
"""
import json
import os
import statistics
import subprocess
import sys
import tempfile

import matplotlib.pyplot as plt

SCRIPT = "taxi_analysis.py"
REPEATS = 3

max_cores = os.cpu_count()
candidates = [1, 2, 4, 6, 8, 10, 12, 16]
CORES = sorted({c for c in candidates if c <= max_cores} | {max_cores})
print(f"Logical cores detected: {max_cores}  ->  testing {CORES}")


def run(cores):
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        out = tmp.name
    subprocess.run(
        [sys.executable, SCRIPT, "--cores", str(cores), "--out", out],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    with open(out) as f:
        data = json.load(f)
    os.remove(out)
    return data


# Warm-up (discarded): fills the OS file cache and warms the JVM
print("Warm-up run (discarded)...")
run(CORES[-1])

results = {}  # cores -> {study: median time}
meta = {}
for c in CORES:
    runs = []
    for i in range(REPEATS):
        print(f"cores={c}  repetition {i + 1}/{REPEATS}")
        data = run(c)
        runs.append(data["timings"])
        meta[c] = data
    studies = runs[0].keys()
    results[c] = {s: statistics.median(r[s] for r in runs) for s in studies}
    results[c]["TOTAL"] = sum(v for k, v in results[c].items() if k != "load_count")

studies = list(results[CORES[0]].keys())

# Speed-up and efficiency
speedup = {s: [results[CORES[0]][s] / results[c][s] for c in CORES] for s in studies}
efficiency = {s: [speedup[s][i] / CORES[i] for i in range(len(CORES))] for s in studies}

# Tables
print("\nTOTAL TIME")
print(f"{'Cores':<6} | {'Time (s)':<9} | {'Speed-up':<9} | {'Efficiency':<10} | Input partitions")
print("-" * 62)
for i, c in enumerate(CORES):
    print(f"{c:<6} | {results[c]['TOTAL']:<9.2f} | {speedup['TOTAL'][i]:<9.2f} | "
          f"{efficiency['TOTAL'][i]:<10.2f} | {meta[c]['input_partitions']}")

# CSV with everything
with open("benchmark_results.csv", "w") as f:
    f.write("study,cores,time_s,speedup,efficiency\n")
    for s in studies:
        for i, c in enumerate(CORES):
            f.write(f"{s},{c},{results[c][s]:.3f},{speedup[s][i]:.3f},{efficiency[s][i]:.3f}\n")

# Plots
plot_studies = [s for s in studies if s not in ("load_count",)]

plt.figure(figsize=(10, 7))
for s in plot_studies:
    style = dict(linewidth=3, color="black") if s == "TOTAL" else dict(linewidth=1.2, alpha=0.8)
    plt.plot(CORES, speedup[s], marker="o", label=s, **style)
plt.plot(CORES, CORES, "r--", linewidth=2, label="Ideal S(p) = p")
plt.title("Speed-up", fontweight="bold")
plt.xlabel("Number of cores")
plt.ylabel("Speed-up  T(1) / T(p)")
plt.xticks(CORES)
plt.grid(True, linestyle=":", alpha=0.7)
plt.legend(fontsize=8, ncol=2)
plt.savefig("speedup_plot.png", dpi=300, bbox_inches="tight")

plt.figure(figsize=(10, 7))
for s in plot_studies:
    style = dict(linewidth=3, color="black") if s == "TOTAL" else dict(linewidth=1.2, alpha=0.8)
    plt.plot(CORES, efficiency[s], marker="o", label=s, **style)
plt.axhline(1.0, color="r", linestyle="--", linewidth=2, label="Ideal E = 1")
plt.title("Efficiency", fontweight="bold")
plt.xlabel("Number of cores")
plt.ylabel("Efficiency  S(p) / p")
plt.xticks(CORES)
plt.grid(True, linestyle=":", alpha=0.7)
plt.legend(fontsize=8, ncol=2)
plt.savefig("efficiency_plot.png", dpi=300, bbox_inches="tight")

print("\nSaved: benchmark_results.csv, speedup_plot.png, efficiency_plot.png")