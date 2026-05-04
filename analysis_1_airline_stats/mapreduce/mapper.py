#!/usr/bin/env python3
"""
Mapper Analisi 3.1 — Statistiche compagnie aeree
Input:  CSV pulito (da stdin, riga per riga)
Output: chiave<TAB>valore
Chiave: carrier|origin|month
Valore: arr_delay|cancelled
"""
import sys

HEADER = None

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Salta l'header
    if line.startswith("fl_date") or line.startswith("year"):
        continue

    fields = line.split(",")

    # Indici colonne del CSV cleaned:
    # 0:fl_date, 1:year, 2:month, 3:op_unique_carrier,
    # 4:origin, 5:dest, 6:dep_delay, 7:arr_delay,
    # 8:cancelled, 9:cancellation_code, 10:carrier_delay,
    # 11:weather_delay, 12:nas_delay, 13:security_delay, 14:late_aircraft_delay
    try:
        month     = fields[2].strip()
        carrier   = fields[3].strip()
        origin    = fields[4].strip()
        arr_delay = fields[7].strip()
        cancelled = fields[8].strip()
    except IndexError:
        continue

    # Salta record con chiavi mancanti
    if not carrier or not origin or not month:
        continue

    # Normalizza arr_delay: se volo cancellato o NaN → stringa "NULL"
    arr_delay = arr_delay if arr_delay not in ("", "nan", "NA") else "NULL"
    cancelled = cancelled if cancelled in ("0", "1") else "0"

    key   = f"{carrier}|{origin}|{month}"
    value = f"{arr_delay}|{cancelled}"

    print(f"{key}\t{value}")