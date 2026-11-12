"""
NYC Taxi analysis with Spark (Parquet, several months).

Usage:
    python taxi_analysis.py                  # reads ./data, uses all cores (local[*])
    python taxi_analysis.py --cores 4        # uses 4 cores
    python taxi_analysis.py --cores 4 --out timings_4.json
    python taxi_analysis.py --data some_folder_or_file.parquet

Studies
    1   Average speed per pickup hour            (DataFrame)
    2   Most common trips (same metric, 3 APIs)  (DataFrame, SQL, RDD)
    3   Passengers vs. distance                  (RDD)
    4   Financial: avg tip per passenger count   (DataFrame, SQL, RDD)
        + avg total cost per payment type        (SQL)
"""
import argparse
import glob
import json
import os
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

DATA_PATH = "data"  # folder with the monthly .parquet files (see download_data.py)

PAYMENT_TYPES = {
    0: "Flex fare", 1: "Credit card", 2: "Cash", 3: "No charge",
    4: "Dispute", 5: "Unknown", 6: "Voided",
}

timings = {} 

# helpers

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--cores", default="*", help="number of cores or '*' for all")
    p.add_argument("--data", default=DATA_PATH, help="folder with .parquet files, or a single file")
    p.add_argument("--out", default=None, help="JSON file where timings are saved")
    return p.parse_args()


def build_spark(cores):
    spark = (
        SparkSession.builder
        .appName(f"NYCTaxi_{cores}_cores")
        .master(f"local[{cores}]")
        .config("spark.sql.shuffle.partitions", "16")  # default 200 is too many for local mode
        .config("spark.driver.memory", "4g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def data_info(path):
    """Devuelve (lista_de_archivos, tamaño_total_en_MB) para una carpeta, patrón o archivo individual."""
    if os.path.isdir(path):
        files = [os.path.join(path, f) for f in sorted(os.listdir(path)) if f.endswith(".parquet")]
    elif "*" in path:
        files = sorted(glob.glob(path))
    else:
        files = [path]
    
    files = [f for f in files if os.path.isfile(f)]
    total_mb = sum(os.path.getsize(f) for f in files) / 1e6 if files else 0.0
    return files, total_mb


def timed(name, fn):
    """Run fn(), store its execution time in `timings` and return its result."""
    t0 = time.time()
    result = fn()
    timings[name] = round(time.time() - t0, 3)
    print(f"   -> {name}: {timings[name]} s")
    return result


def section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

# Study 1: average speed per hour (DataFrame API)

def study1_speed(df):
    df_speed = (
        df.withColumn(
            "duration_hours",
            (F.unix_timestamp("tpep_dropoff_datetime")
             - F.unix_timestamp("tpep_pickup_datetime")) / 3600)
        # Remove outliers: trips shorter than 1 minute or with no distance
        .filter((F.col("duration_hours") >= 1 / 60) & (F.col("trip_distance") > 0))
        .withColumn("speed_mph", F.col("trip_distance") / F.col("duration_hours"))
        # Remove impossible speeds
        .filter((F.col("speed_mph") > 0) & (F.col("speed_mph") <= 80))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    )
    return (
        df_speed.groupBy("pickup_hour")
        .agg(F.avg("speed_mph").alias("avg_speed_mph"))
        .orderBy("pickup_hour")
        .collect()
    )

# Study 2: most common trips, same metric with the three APIs

def study2_df(df):
    return (
        df.groupBy("PULocationID", "DOLocationID").count()
        .orderBy(F.desc("count"))
        .limit(10)
        .collect()
    )


def study2_sql(spark, df):
    df.createOrReplaceTempView("taxi_trips")
    return spark.sql("""
        SELECT PULocationID, DOLocationID, COUNT(*) AS trip_count
        FROM taxi_trips
        GROUP BY PULocationID, DOLocationID
        ORDER BY trip_count DESC
        LIMIT 10
    """).collect()


def study2_rdd(df):
    return (
        df.select("PULocationID", "DOLocationID").rdd
        .map(lambda row: ((row[0], row[1]), 1))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(10, key=lambda x: -x[1])  # avoids sorting the whole RDD
    )

# Study 3: passenger count vs. distance (RDD API)
def study3_rdd(df):
    return (
        df.select("passenger_count", "trip_distance").rdd
        .filter(lambda r: r[0] is not None and 0 < r[0] <= 6
                and r[1] is not None and r[1] > 0)
        .map(lambda r: (int(r[0]), (float(r[1]), 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda s: (s[1], round(s[0] / s[1], 2)))
        .sortByKey()
        .collect()
    )

# Study 4: financial. Avg tip per passenger count with the three APIs

def clean_financial(df):
    return df.filter(
        (F.col("passenger_count") > 0)
        & (F.col("tip_amount") >= 0)
        & (F.col("total_amount") > 0)  # removes refunds / disputes
    )


def study4_df(df_clean):
    rows = (
        df_clean.groupBy("passenger_count")
        .agg(F.avg("tip_amount").alias("avg_tip"))
        .orderBy("passenger_count")
        .collect()
    )
    return {int(r["passenger_count"]): r["avg_tip"] for r in rows}


def study4_sql(spark, df_clean):
    df_clean.createOrReplaceTempView("taxi_clean")
    rows = spark.sql("""
        SELECT passenger_count, AVG(tip_amount) AS avg_tip
        FROM taxi_clean
        GROUP BY passenger_count
        ORDER BY passenger_count
    """).collect()
    return {int(r["passenger_count"]): r["avg_tip"] for r in rows}


def study4_rdd(df_clean):
    rows = (
        df_clean.select("passenger_count", "tip_amount").rdd
        .map(lambda r: (int(r[0]), (float(r[1]), 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda s: s[0] / s[1])
        .sortByKey()
        .collect()
    )
    return dict(rows)


def study4_payment_sql(spark):
    # Uses the "taxi_clean" view registered in study4_sql
    return spark.sql("""
        SELECT payment_type, COUNT(*) AS trips, AVG(total_amount) AS avg_total
        FROM taxi_clean
        GROUP BY payment_type
        ORDER BY avg_total DESC
    """).collect()


# Main
def main():
    args = parse_args()
    spark = build_spark(args.cores)

    # Obtenemos la lista de todos los archivos .parquet
    files, file_mb = data_info(args.data)
    n_files = len(files)

    if not files:
        raise FileNotFoundError(f"No se encontraron archivos .parquet en la ruta: {args.data}")

    section(f"DATA INGESTION  |  cores = {args.cores}")
    
    # Pasamos la lista completa de archivos desempaquetada a Spark
    df = spark.read.parquet(*files)
    n_partitions = df.rdd.getNumPartitions()

    total = timed("load_count", lambda: df.count())
    timed("load_scan", lambda: df.agg(F.sum("trip_distance")).collect())
    print(f"   Data: {args.data} ({n_files} file(s), {file_mb:.1f} MB)")
    print(f"   Records: {total}")
    print(f"   Input partitions: {n_partitions}")

    # Study 1
    section("STUDY 1: Average speed per pickup hour (DataFrame)")
    rows = timed("1_speed_df", lambda: study1_speed(df))
    for r in rows:
        print(f"   {r['pickup_hour']:02d}h  {r['avg_speed_mph']:.2f} mph")

    # Study 2
    section("STUDY 2: Top 10 most common trips (PULocationID -> DOLocationID)")
    r_df = timed("2a_trips_df", lambda: study2_df(df))
    r_sql = timed("2b_trips_sql", lambda: study2_sql(spark, df))
    r_rdd = timed("2c_trips_rdd", lambda: study2_rdd(df))
    print("   DataFrame:")
    for r in r_df:
        print(f"     {r['PULocationID']:>4} -> {r['DOLocationID']:<4} {r['count']}")
    print("   Same top 10 in SQL?", [(r[0], r[1]) for r in r_sql] == [(r[0], r[1]) for r in r_df])
    print("   RDD top 3:", r_rdd[:3])

    # Study 3
    section("STUDY 3: Passenger count vs. average distance (RDD)")
    summary = timed("3_pass_dist_rdd", lambda: study3_rdd(df))
    print(f"   {'Passengers':<12} | {'Trips':<10} | {'Avg distance (miles)'}")
    for p, (trips, dist) in summary:
        print(f"   {p:<12} | {trips:<10} | {dist}")

    # Study 4
    section("STUDY 4: Avg tip per passenger count (DataFrame vs SQL vs RDD)")
    df_clean = clean_financial(df)
    n_clean = df_clean.count()
    print(f"   Records after cleaning: {n_clean}")
    t_df = timed("4a_tip_df", lambda: study4_df(df_clean))
    t_sql = timed("4b_tip_sql", lambda: study4_sql(spark, df_clean))
    t_rdd = timed("4c_tip_rdd", lambda: study4_rdd(df_clean))

    same = all(
        abs(t_df[k] - t_sql[k]) < 1e-6 and abs(t_df[k] - t_rdd[k]) < 1e-6
        for k in t_df
    )
    print("   The three APIs give the same result?", same)
    for k in sorted(t_df):
        print(f"   {k} passengers -> ${t_df[k]:.2f}")

    print("\n   Avg total cost per payment type (SQL):")
    pay = timed("4d_payment_sql", lambda: study4_payment_sql(spark))
    for r in pay:
        name = PAYMENT_TYPES.get(r["payment_type"], str(r["payment_type"]))
        print(f"   {name:<12} | trips: {r['trips']:<9} | avg total: ${r['avg_total']:.2f}")
    print("   NOTE: tip_amount is only recorded for card payments (cash tips = 0).")

    # Summary
    section("TIMING SUMMARY")
    for name, t in timings.items():
        print(f"   {name:<18} {t:>8.3f} s")

    if args.out:
        with open(args.out, "w") as f:
            json.dump({
                "cores": args.cores,
                "records": total,
                "records_after_financial_filter": n_clean,
                "files": n_files,
                "file_mb": round(file_mb, 1),
                "input_partitions": n_partitions,
                "timings": timings,
            }, f, indent=2)

    spark.stop()


if __name__ == "__main__":
    main()