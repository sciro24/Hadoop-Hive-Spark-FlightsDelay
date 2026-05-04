import pandas as pd
import numpy as np
import os

# ─── Paths ────────────────────────────────────────────────────────────────────
RAW_PATH     = "data/raw/flight_data_2024.csv"
CLEANED_PATH = "data/cleaned/flight_data_2024_cleaned.csv"
os.makedirs("data/cleaned", exist_ok=True)

# ─── 1. Caricamento ───────────────────────────────────────────────────────────
print("Caricamento dataset...")
df = pd.read_csv(RAW_PATH, low_memory=False)
print(f"Shape iniziale: {df.shape}")

# ─── 2. Selezione colonne rilevanti ───────────────────────────────────────────
COLS = [
    "fl_date",            # Data del volo
    "year",               # Anno (già presente)
    "month",              # Mese (già presente)
    "op_unique_carrier",  # Codice IATA compagnia
    "origin",             # Aeroporto di partenza
    "dest",               # Aeroporto di arrivo
    "dep_delay",          # Ritardo in partenza (minuti)
    "arr_delay",          # Ritardo in arrivo (minuti)
    "cancelled",          # Flag cancellazione (1.0 = cancellato)
    "cancellation_code",  # Causa: A=Carrier, B=Weather, C=NAS, D=Security
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay"
]

COLS = [c for c in COLS if c in df.columns]
df = df[COLS]
print(f"Colonne selezionate: {len(COLS)}")

# ─── 3. Parsing fl_date ───────────────────────────────────────────────────────
df["fl_date"] = pd.to_datetime(df["fl_date"], errors="coerce")

# ─── 4. Rimozione record con chiavi obbligatorie mancanti ─────────────────────
mandatory = ["fl_date", "op_unique_carrier", "origin"]
before = len(df)
df.dropna(subset=mandatory, inplace=True)
print(f"Rimossi {before - len(df)} record con chiavi mancanti")

# ─── 5. Gestione voli cancellati ──────────────────────────────────────────────
df["cancelled"] = df["cancelled"].fillna(0).astype(int)

# ─── 6. Normalizzazione ritardi ───────────────────────────────────────────────
delay_cols = ["dep_delay", "arr_delay", "carrier_delay",
              "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"]

for col in delay_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Rimozione outlier anomali fuori da [-120, 1440] minuti
for col in ["dep_delay", "arr_delay"]:
    before = len(df)
    df = df[(df[col].isna()) | ((df[col] >= -120) & (df[col] <= 1440))]
    print(f"Rimossi {before - len(df)} record con {col} fuori range [-120, 1440]")

# ─── 7. Normalizzazione stringhe ──────────────────────────────────────────────
for col in ["op_unique_carrier", "origin", "dest", "cancellation_code"]:
    if col in df.columns:
        df[col] = df[col].str.strip().str.upper()

# ─── 8. Rimozione duplicati esatti ────────────────────────────────────────────
before = len(df)
df.drop_duplicates(inplace=True)
print(f"Rimossi {before - len(df)} duplicati esatti")

# ─── 9. Reset index e riepilogo finale ────────────────────────────────────────
df.reset_index(drop=True, inplace=True)
print(f"\nShape finale: {df.shape}")
print(f"\nNull per colonna:\n{df.isnull().sum()}")

# ─── 10. Salvataggio ──────────────────────────────────────────────────────────
df.to_csv(CLEANED_PATH, index=False)
print(f"\nDataset pulito salvato in: {CLEANED_PATH}")