"""
ANALISI 3.1 - Statistiche delle compagnie aeree
Tecnologia: Spark SQL
Confronto con: Hive (MapReduce)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import os

# ─────────────────────────────────────────
# CONFIGURAZIONE
# ─────────────────────────────────────────
HDFS_BASE   = "hdfs://localhost:9000/user/diego/flights"
OUTPUT_BASE = "results/analysis1/sparksql"
os.makedirs(OUTPUT_BASE, exist_ok=True)

spark = SparkSession.builder \
    .appName("Analysis3.1_SparkSQL") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("="*60)
print("ANALISI 3.1 - SPARK SQL")
print("="*60)

# ─────────────────────────────────────────
# CARICA DATASET PULITO
# ─────────────────────────────────────────
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(f"{HDFS_BASE}/clean")

df.createOrReplaceTempView("flights")

# ─────────────────────────────────────────
# ANALISI SU DATASET COMPLETO
# ─────────────────────────────────────────
print("\n[1/2] Esecuzione query su dataset completo (7M record)...")
t0 = time.time()

result = spark.sql("""
    SELECT
        op_unique_carrier                              AS airline,
        origin                                         AS airport,
        COUNT(*)                                       AS total_flights,
        ROUND(MIN(arr_delay), 2)                       AS min_arr_delay,
        ROUND(MAX(arr_delay), 2)                       AS max_arr_delay,
        ROUND(AVG(arr_delay), 2)                       AS avg_arr_delay,
        ROUND(SUM(cancelled) / COUNT(*) * 100, 2)      AS cancellation_rate_pct,
        COLLECT_SET(CAST(month AS STRING))             AS active_months
    FROM flights
    GROUP BY op_unique_carrier, origin
    ORDER BY op_unique_carrier, origin
""")

# Converti array in stringa per salvataggio CSV
result_csv = result.withColumn(
    "active_months",
    F.array_join(F.col("active_months"), "|")
)
result_csv.write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"{HDFS_BASE}/results/analysis1_sparksql")

t1 = time.time()
elapsed_full = t1 - t0
print(f"      Tempo dataset completo: {elapsed_full:.2f}s")

# Prime 10 righe in locale
result_local = result.limit(10).toPandas()
result_local.to_csv(f"{OUTPUT_BASE}/top10_full.csv", index=False)
print(f"      Prime 10 righe salvate in {OUTPUT_BASE}/top10_full.csv")

# ─────────────────────────────────────────
# BENCHMARK SUI SAMPLE
# ─────────────────────────────────────────
print("\n[2/2] Benchmark su sample crescenti...")
samples = [("10%", "sample_10pct"), ("25%", "sample_25pct"), ("50%", "sample_50pct")]
benchmark_rows = []

for label, folder in samples:
    df_s = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .csv(f"{HDFS_BASE}/samples/{folder}")
    df_s.createOrReplaceTempView("flights_sample")

    t0 = time.time()
    spark.sql("""
        SELECT op_unique_carrier, origin,
               COUNT(*) AS total_flights,
               ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
               ROUND(SUM(cancelled)/COUNT(*)*100, 2) AS cancellation_rate_pct
        FROM flights_sample
        GROUP BY op_unique_carrier, origin
    """).count()
    elapsed = time.time() - t0
    benchmark_rows.append((label, elapsed))
    print(f"      Sample {label}: {elapsed:.2f}s")

benchmark_rows.append(("100%", elapsed_full))

# Salva benchmark
import csv
with open(f"benchmarks/analysis1_sparksql.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["sample_size", "spark_sql_seconds", "hive_seconds"])
    hive_time = 48.1
    for label, t in benchmark_rows:
        writer.writerow([label, round(t, 2), hive_time if label == "100%" else ""])

print(f"\n      Benchmark salvato in benchmarks/analysis1_sparksql.csv")

print("\n" + "="*60)
print(f"ANALISI 3.1 SPARK SQL COMPLETATA in {elapsed_full:.2f}s")
print(f"Hive (MapReduce):  48.1s")
print(f"Speedup Spark SQL: {48.1/elapsed_full:.1f}x più veloce")
print("="*60)

spark.stop()