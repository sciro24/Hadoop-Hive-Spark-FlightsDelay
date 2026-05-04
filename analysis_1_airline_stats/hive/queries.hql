-- ─── Analisi 3.1 — Statistiche Compagnie Aeree ───────────────────────────────
-- Tecnologia: Apache Hive 2.3.9
-- Input: dataset pulito caricato come tabella esterna

-- ─── 1. Crea database (se non esiste) ────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS flights;
USE flights;

-- ─── 2. Drop tabella se già esiste ───────────────────────────────────────────
DROP TABLE IF EXISTS flights_clean;

-- ─── 3. Tabella esterna sul CSV pulito ───────────────────────────────────────
CREATE EXTERNAL TABLE flights_clean (
    fl_date             STRING,
    year                INT,
    month               INT,
    op_unique_carrier   STRING,
    origin              STRING,
    dest                STRING,
    dep_delay           DOUBLE,
    arr_delay           DOUBLE,
    cancelled           INT,
    cancellation_code   STRING,
    carrier_delay       DOUBLE,
    weather_delay       DOUBLE,
    nas_delay           DOUBLE,
    security_delay      DOUBLE,
    late_aircraft_delay DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/flights_clean'
TBLPROPERTIES ("skip.header.line.count"="1");

-- ─── 4. Tabella risultati ─────────────────────────────────────────────────────
DROP TABLE IF EXISTS results_airline_stats;

CREATE TABLE results_airline_stats AS
SELECT
    op_unique_carrier                           AS carrier,
    origin,
    month,
    COUNT(*)                                    AS num_flights,
    ROUND(MIN(arr_delay), 2)                    AS min_arr_delay,
    ROUND(MAX(arr_delay), 2)                    AS max_arr_delay,
    ROUND(AVG(arr_delay), 2)                    AS avg_arr_delay,
    ROUND(SUM(cancelled) / COUNT(*), 4)         AS cancel_rate,
    COLLECT_SET(CAST(month AS STRING))          AS active_months
FROM flights_clean
WHERE op_unique_carrier IS NOT NULL
  AND origin            IS NOT NULL
  AND month             IS NOT NULL
GROUP BY
    op_unique_carrier,
    origin,
    month;

-- ─── 5. Mostra prime 10 righe ─────────────────────────────────────────────────
SELECT * FROM results_airline_stats
ORDER BY carrier, origin, month
LIMIT 10;