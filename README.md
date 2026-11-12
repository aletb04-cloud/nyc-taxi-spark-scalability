# NYC Taxi: Big Data Processing & Scalability Study with Apache Spark

Performance and scalability study of Apache Spark on **48.7 million NYC Yellow Taxi trips** (12 months of 2025, 830 MB in Parquet), run in local mode on a single laptop (MacBook Air M2, 8 GB RAM). Developed for the *High-Performance Computing for Big Data Analysis* course of the MSc in Big Data Analytics (UC3M).

## What this project does

1. **Traffic and financial analytics**: average speed per pickup hour, most frequent pickup-dropoff routes, trip distance per passenger count, and tips and fares by payment type.
2. **API comparison**: the same computations (top routes, average tip per passenger count) implemented with the **DataFrame**, **Spark SQL** and **RDD** APIs. The results are identical; the execution time is not.
3. **Scalability benchmark**: execution time, speed-up and efficiency with 1, 2, 4, 6 and 8 cores (one discarded warm-up run, median of 3 runs per configuration).

## Key results

Measured with `benchmark.py` (raw numbers in [`results/benchmark_results.csv`](results/benchmark_results.csv)):

| Cores | Total time (s) | Speed-up | Efficiency |
|---|---|---|---|
| 1 | 187.89 | 1.00 | 1.00 |
| 2 | 135.11 | 1.39 | 0.70 |
| 4 | 155.78 | 1.21 | 0.30 |
| 6 | 164.47 | 1.14 | 0.19 |
| 8 | 140.86 | 1.33 | 0.17 |

- **RDD is 25-27x slower than DataFrame/SQL on one core** (25-44x depending on the number of cores) for identical results. The cause is the JVM-to-Python serialization of every row and the absence of a query optimizer. DataFrame and Spark SQL perform the same.
- **The three RDD studies account for 92% of the single-core time**, so the total speed-up essentially follows the RDD behaviour.
- **Scaling is far from linear.** The DataFrame/SQL studies keep improving up to 8 cores (1.57x), while the RDD studies peak at 1.40x with 2 cores and then fluctuate.
- Only 10-12 input partitions and an Apple M2 with 4 performance + 4 efficiency cores, 8 GB of RAM and passive cooling are plausible limiting factors. They were not isolated in this study.

### A note on variability

The original course report (run from a Jupyter notebook, where all configurations share one Python process and a warm JVM) measured a peak speed-up of 1.80x with 4 cores. This repository launches a fresh process per run and measured a peak of 1.39x with 2 cores, with a non-monotonic curve in the RDD studies. Single-core times agree (186 s vs 188 s). The conclusion is the same in both runs: sub-linear scaling, dominated by the RDD studies, and no gain beyond a few cores on this hardware. Differences in the multi-core numbers are consistent with run-to-run noise and thermal behaviour on a fanless laptop, which was not measured separately.

## Limitations

- Single laptop in local mode: the results say nothing definitive about scaling on a real cluster.
- `tip_amount` is only recorded for card payments (cash tips are stored as 0), so the average tips are underestimated.
- Groups of 7-9 passengers are too small for reliable averages.

## Repository structure

```
.
├── download_data.py    # downloads the monthly Parquet files from the TLC
├── taxi_analysis.py    # the four studies; prints results and timings
├── benchmark.py        # runs taxi_analysis.py with 1-8 cores, writes results and plots
├── results/            # benchmark_results.csv and plots
├── requirements.txt
└── README.md
```

## How to reproduce

**Requirements:** Python 3.11 and a Java JDK (17 recommended), which PySpark needs.

```bash
git clone https://github.com/aletb04-cloud/nyc-taxi-spark-scalability.git
cd nyc-taxi-spark-scalability
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python download_data.py --start 2025-01 --months 12   # ~830 MB into ./data
python taxi_analysis.py --cores 4                     # one run of all studies
python benchmark.py                                   # full benchmark (about 40-60 min)
```

`benchmark.py` writes `benchmark_results.csv`, `speedup_plot.png` and `efficiency_plot.png` in the current folder. For reliable timings, plug in the laptop, close other applications and do not use it during the run.

## Data

NYC Taxi & Limousine Commission (TLC) Trip Record Data, Yellow Taxi, January-December 2025. Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Authors

Alejandro Trébol Barreno and Jorge Ranz Díaz de Rojas.
