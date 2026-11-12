# NYC Taxi: Big Data Processing & Scalability Study with Apache Spark

Performance and scalability study of Apache Spark on **48.7 million NYC Yellow Taxi trips** (12 months of 2025, 830 MB in Parquet), run in local mode on a single laptop. Developed for the *High-Performance Computing for Big Data Analysis* course of the MSc in Big Data Analytics (UC3M).

## What this project does

1. **Traffic and financial analytics**: average speed per pickup hour, most frequent pickup-dropoff routes, trip distance per passenger count, and tips and fares by payment type.
2. **API comparison**: the same computations (top routes, average tip per passenger count) implemented with the **DataFrame**, **Spark SQL** and **RDD** APIs. Results are identical; execution time is not.
3. **Scalability benchmark**: execution time, speed-up and efficiency with 1, 2, 4, 6 and 8 cores (warm-up run discarded, median of 3 runs), plus the effect of data volume (1 month vs 12 months).

## Key results

| Finding | Result |
|---|---|
| Records processed | 48,722,602 (12 monthly Parquet files) |
| RDD vs DataFrame/SQL | RDD is **24-28x slower** for identical results (Python serialization, no query optimizer) |
| Best speed-up (whole workload) | **1.80x with 4 cores** (efficiency 0.45) |
| Beyond 4 cores | Speed-up drops to 1.19x with 8 cores |
| Data volume | 14x more data -> 11.5x more single-core time; speed-up and efficiency improve with larger input |

The degradation beyond 4 cores is consistent with the hardware (Apple M2: 4 performance + 4 efficiency cores, 8 GB RAM, passive cooling) and with having only 10-12 input partitions. The report separates what was measured from what is a plausible, untested explanation (memory pressure, thermal throttling). Amdahl's law is used to interpret the sequential fraction.

## Limitations

- Single laptop in local mode: the results do not prove scaling on a real cluster.
- `tip_amount` is only recorded for card payments (cash tips are stored as 0), so average tips are underestimated.
- Groups with 7-9 passengers are too small for reliable averages.

## Repository structure

```
.
├── project_taxis.ipynb   # full analysis and benchmark
├── docs/                 # project report (PDF)
├── requirements.txt
└── README.md
```

## How to reproduce

**Requirements:** Python 3.11 and a Java JDK (17 recommended), which PySpark needs.

```bash
git clone https://github.com/aletb04-cloud/nyc-taxi-spark-scalability.git
cd NOMBRE-DEL-REPO
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook project_taxis.ipynb
```

The notebook downloads the monthly Parquet files from the NYC TLC into a local `data/` folder (not included in the repository). By default it is set to a few months for quick testing; to reproduce the report, set `months=12` in the `download_dataset(...)` call (start `2025-01`) and run all cells. The benchmark takes a while on 12 months because the RDD studies are slow by design.

## Data

NYC Taxi & Limousine Commission (TLC) Trip Record Data, Yellow Taxi, January-December 2025. Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Authors

Alejandro Trébol Barreno and Jorge Ranz Díaz de Rojas.
