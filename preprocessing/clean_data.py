"""
Preprocessing del Flight Delay Dataset 2024
- Rimozione record incompleti/errati
- Normalizzazione attributi
- Selezione colonne rilevanti
- Generazione sample dataset per benchmark
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType
import time

# ─────────────────────────────────────────
# CONFIGURAZIONE
# ─────────────────────────────────────────
RAW_PATH    = "hdfs://localhost:9000/user/diego/flights/raw/flight_data_2024.csv"
CLEAN_PATH  = "hdfs://localhost:9000/user/diego/flights/clean"
SAMPLE_PATH = "hdfs://localhost:9000/user/diego/flights/samples"

# ─────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("FlightDelay_Preprocessing") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("="*60)
print("PREPROCESSING FLIGHT DELAY DATASET 2024")
print("="*60)

# ─────────────────────────────────────────
# 1. CARICA DATASET GREZZO
# ─────────────────────────────────────────
print("\n[1/5] Caricamento dataset grezzo...")
df_raw = spark.read.csv(RAW_PATH, header=True, inferSchema=True)
total_raw = df_raw.count()
print(f"      Record totali: {total_raw:,}")

# ─────────────────────────────────────────
# 2. SELEZIONE COLONNE RILEVANTI
# ─────────────────────────────────────────
print("\n[2/5] Selezione colonne rilevanti...")
COLS = [
    "year", "month", "day_of_month", "day_of_week", "fl_date",
    "op_unique_carrier",   # codice compagnia
    "origin",              # aeroporto partenza
    "origin_city_name",
    "dest",                # aeroporto destinazione
    "dest_city_name",
    "dep_delay",           # ritardo partenza (min)
    "arr_delay",           # ritardo arrivo (min)
    "cancelled",           # 0/1
    "cancellation_code",   # A=Carrier, B=Weather, C=NAS, D=Security
    "diverted",
    "distance",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay"
]
df = df_raw.select(COLS)

# ─────────────────────────────────────────
# 3. PULIZIA E NORMALIZZAZIONE
# ─────────────────────────────────────────
print("\n[3/5] Pulizia e normalizzazione...")

# Cast tipi espliciti
df = df \
    .withColumn("year",             F.col("year").cast(IntegerType())) \
    .withColumn("month",            F.col("month").cast(IntegerType())) \
    .withColumn("day_of_month",     F.col("day_of_month").cast(IntegerType())) \
    .withColumn("day_of_week",      F.col("day_of_week").cast(IntegerType())) \
    .withColumn("fl_date",          F.col("fl_date").cast("string")) \
    .withColumn("dep_delay",        F.col("dep_delay").cast(FloatType())) \
    .withColumn("arr_delay",        F.col("arr_delay").cast(FloatType())) \
    .withColumn("cancelled",        F.col("cancelled").cast(IntegerType())) \
    .withColumn("diverted",         F.col("diverted").cast(IntegerType())) \
    .withColumn("distance",         F.col("distance").cast(FloatType())) \
    .withColumn("carrier_delay",    F.col("carrier_delay").cast(FloatType())) \
    .withColumn("weather_delay",    F.col("weather_delay").cast(FloatType())) \
    .withColumn("nas_delay",        F.col("nas_delay").cast(FloatType())) \
    .withColumn("security_delay",   F.col("security_delay").cast(FloatType())) \
    .withColumn("late_aircraft_delay", F.col("late_aircraft_delay").cast(FloatType()))

# Normalizzazione: codici in UPPERCASE, trim spazi
df = df \
    .withColumn("op_unique_carrier", F.upper(F.trim(F.col("op_unique_carrier")))) \
    .withColumn("origin",            F.upper(F.trim(F.col("origin")))) \
    .withColumn("dest",              F.upper(F.trim(F.col("dest"))))

# Rimozione record con campi critici nulli
df_clean = df.filter(
    F.col("op_unique_carrier").isNotNull() &
    F.col("origin").isNotNull() &
    F.col("dest").isNotNull() &
    F.col("month").isNotNull() &
    F.col("cancelled").isNotNull() &
    # I voli non cancellati DEVONO avere dep_delay e arr_delay
    (
        (F.col("cancelled") == 1) |
        (F.col("dep_delay").isNotNull() & F.col("arr_delay").isNotNull())
    )
)

# Rimozione record con anno errato (solo 2024)
df_clean = df_clean.filter(F.col("year") == 2024)

# Rimozione ritardi fuori range plausibile (-120 min ÷ 1500 min)
df_clean = df_clean.filter(
    (F.col("cancelled") == 1) |
    (
        (F.col("dep_delay") >= -120) & (F.col("dep_delay") <= 1500) &
        (F.col("arr_delay") >= -120) & (F.col("arr_delay") <= 1500)
    )
)

total_clean = df_clean.count()
removed = total_raw - total_clean
print(f"      Record dopo pulizia: {total_clean:,}")
print(f"      Record rimossi:      {removed:,} ({removed/total_raw*100:.2f}%)")

# ─────────────────────────────────────────
# 4. SALVA DATASET PULITO SU HDFS 
# ─────────────────────────────────────────
print("\n[4/5] Salvataggio dataset pulito su HDFS in formato CSV...")
df_clean.write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(CLEAN_PATH)
print(f"      Salvato in: {CLEAN_PATH}")

# ─────────────────────────────────────────
# 5. GENERA SAMPLE PER BENCHMARK
# ─────────────────────────────────────────
print("\n[5/5] Generazione sample per benchmark...")
fractions = [0.10, 0.25, 0.50]
for frac in fractions:
    pct = int(frac * 100)
    sample_path = f"{SAMPLE_PATH}/sample_{pct}pct"
    df_sample = df_clean.sample(withReplacement=False, fraction=frac, seed=42)
    df_sample.write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .csv(sample_path)
    n = df_sample.count()
    print(f"      Sample {pct:3d}%: {n:>8,} record → {sample_path}")