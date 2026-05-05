#!/usr/bin/env python3
"""
Analisi 3.2 — Report Ritardi per Aeroporto e Periodo Temporale
Tecnologia: Spark Core 3.5.8 (RDD API)
"""
import os, sys, time, glob, shutil
from pathlib import Path
from pyspark import SparkContext, SparkConf

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH   = sys.argv[1] if len(sys.argv) > 1 else str(PROJECT_ROOT / "data" / "cleaned" / "flight_data_2024_cleaned.csv")
OUTPUT_PATH  = str(PROJECT_ROOT / "results" / "analysis_2" / "spark_core")
os.makedirs(OUTPUT_PATH, exist_ok=True)

for folder in ["delay_report_raw", "delay_causes_raw"]:
    p = os.path.join(OUTPUT_PATH, folder)
    if os.path.exists(p):
        shutil.rmtree(p)

conf = SparkConf() \
    .setAppName("Analysis_3.2_DelayReport_SparkCore") \
    .setMaster("local[*]") \
    .set("spark.driver.memory", "4g")

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

print(f"Spark versione: {sc.version}")
print(f"Input: {INPUT_PATH}")

start = time.time()

# ─── 1. Parsing ───────────────────────────────────────────────────────────────
raw    = sc.textFile(INPUT_PATH)
header = raw.first()

def parse_line(line):
    fields = line.split(",")
    if len(fields) < 15:
        return None
    try:
        origin    = fields[4].strip()
        month     = int(fields[2].strip())
        # FIX: dep/arr_delay → None se mancanti (non 0.0)
        def to_float_or_none(s):
            s = s.strip()
            return float(s) if s not in ("", "nan", "NA") else None
        dep_delay = to_float_or_none(fields[6])
        arr_delay = to_float_or_none(fields[7])
        # cause: 0.0 è corretto (volo senza quella causa di ritardo)
        def to_float_zero(s):
            s = s.strip()
            return float(s) if s not in ("", "nan", "NA") else 0.0
        carrier_d  = to_float_zero(fields[10])
        weather_d  = to_float_zero(fields[11])
        nas_d      = to_float_zero(fields[12])
        security_d = to_float_zero(fields[13])
        late_d     = to_float_zero(fields[14])
        if not origin or not month:
            return None
        return (origin, month, dep_delay, arr_delay, carrier_d, weather_d, nas_d, security_d, late_d)
    except (ValueError, IndexError):
        return None

records = raw \
    .filter(lambda line: line != header) \
    .map(parse_line) \
    .filter(lambda x: x is not None)

records.cache()

# ─── 2. Fasce di ritardo ──────────────────────────────────────────────────────
def assign_band(dep_delay):
    if dep_delay is None:
        return "unknown"
    if dep_delay < 15:
        return "low"
    elif dep_delay <= 60:
        return "medium"
    else:
        return "high"

def to_band_kv(rec):
    origin, month, dep_delay, arr_delay, *_ = rec
    band = assign_band(dep_delay)
    # FIX: sep dep/arr_delay → accumuliamo sum e count separatamente
    #      per le unknown dep/arr rimangono None → non entrano nella somma
    dep = dep_delay if dep_delay is not None else 0.0
    arr = arr_delay if arr_delay is not None else 0.0
    dep_valid = 0 if dep_delay is None else 1   # contatore valori non-None
    arr_valid = 0 if arr_delay is None else 1
    return ((origin, month, band), (dep, arr, 1, dep_valid, arr_valid))

def merge_band(a, b):
    return (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3], a[4]+b[4])

def format_avg(total, count):
    """Ritorna None se nessun valore valido, altrimenti media arrotondata."""
    if count == 0:
        return None
    return round(total / count, 2)

delay_bands = records \
    .map(to_band_kv) \
    .reduceByKey(merge_band) \
    .map(lambda kv: (
        kv[0][0],                               # origin
        kv[0][1],                               # month
        kv[0][2],                               # delay_band
        kv[1][2],                               # num_flights
        format_avg(kv[1][0], kv[1][3]),         # avg_dep_delay (None per unknown)
        format_avg(kv[1][1], kv[1][4]),         # avg_arr_delay (None per unknown)
    )) \
    .sortBy(lambda x: (x[0], x[1], x[2]))

# ─── 3. Top 3 cause per (origin, month) ──────────────────────────────────────
CAUSES = ["carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"]

def to_cause_kv(rec):
    origin, month, _, _, carrier_d, weather_d, nas_d, security_d, late_d = rec
    cause_values = [carrier_d, weather_d, nas_d, security_d, late_d]
    return [
        ((origin, month, CAUSES[i]), (val, 1))
        for i, val in enumerate(cause_values) if val > 0
    ]

causes_avg = records \
    .flatMap(to_cause_kv) \
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
    .map(lambda kv: (
        (kv[0][0], kv[0][1]),
        (kv[0][2], round(kv[1][0] / kv[1][1], 4))
    )) \
    .groupByKey() \
    .mapValues(lambda causes: sorted(causes, key=lambda x: -x[1])[:3]) \
    .flatMap(lambda kv: [
        (kv[0][0], kv[0][1], cause, avg_min, rank + 1)
        for rank, (cause, avg_min) in enumerate(kv[1])
    ]) \
    .sortBy(lambda x: (x[0], x[1], x[4]))

# ─── 4. Salvataggio ───────────────────────────────────────────────────────────
def fmt_val(v):
    """Serializza None come stringa vuota (coerente con Spark SQL)."""
    return "" if v is None else str(v)

delay_bands_out = delay_bands.map(
    lambda x: f"{x[0]}|{x[1]}|{x[2]}|{x[3]}|{fmt_val(x[4])}|{fmt_val(x[5])}"
)
delay_bands_out.coalesce(1).saveAsTextFile(f"{OUTPUT_PATH}/delay_report_raw")

causes_out = causes_avg.map(
    lambda x: f"{x[0]}|{x[1]}|{x[2]}|{x[3]}|{x[4]}"
)
causes_out.coalesce(1).saveAsTextFile(f"{OUTPUT_PATH}/delay_causes_raw")

HEADERS = {
    "output_delay_report.csv": "origin|month|delay_band|num_flights|avg_dep_delay|avg_arr_delay",
    "output_delay_causes.csv": "origin|month|cause|avg_minutes|rank_pos",
}
for folder, outfile in [
    ("delay_report_raw", "output_delay_report.csv"),
    ("delay_causes_raw", "output_delay_causes.csv"),
]:
    parts = glob.glob(f"{OUTPUT_PATH}/{folder}/part-*")
    if parts:
        with open(f"{OUTPUT_PATH}/{outfile}", "w") as fout:
            fout.write(HEADERS[outfile] + "\n")
            with open(parts[0], "r") as fin:
                shutil.copyfileobj(fin, fout)
    shutil.rmtree(f"{OUTPUT_PATH}/{folder}", ignore_errors=True)

elapsed = round(time.time() - start, 2)
print(f"\nTempo di esecuzione: {elapsed}s")
print(f"Risultati in: {OUTPUT_PATH}")

print("\n=== Prime 10 righe delay_report ===")
for row in delay_bands.take(10):
    print(row)

print("\n=== Prime 10 righe delay_causes ===")
for row in causes_avg.take(10):
    print(row)

sc.stop()