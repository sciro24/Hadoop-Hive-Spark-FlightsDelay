-- ─── Analisi 3.1 — Statistiche Compagnie Aeree ───────────────────────────────
CREATE DATABASE IF NOT EXISTS flights;
USE flights;

DROP TABLE IF EXISTS flights_clean;

CREATE EXTERNAL TABLE flights_clean (
    fl_date             STRING,
    year                INT,
    month               INT,
    op_unique_carrier   STRING,
    origin              STRING,
    dest                STRING,
    dep_delay           DOUBLE,
    arr_delay           DOUBLE,
    cancelled           DOUBLE,
    cancellation_code   STRING,
    carrier_delay       DOUBLE,
    weather_delay       DOUBLE,
    nas_delay           DOUBLE,
    security_delay      DOUBLE,
    late_aircraft_delay DOUBLE
)
STORED AS PARQUET
LOCATION '${hivevar:DATA_LOCATION}';

DROP TABLE IF EXISTS results_airline_stats;

CREATE TABLE results_airline_stats AS
WITH monthly_stats AS (
    SELECT
        op_unique_carrier,
        origin,
        month,
        COUNT(*)                                AS num_flights,
        ROUND(MIN(arr_delay),  2)                AS min_arr_delay,
        ROUND(MAX(arr_delay),  2)                AS max_arr_delay,
        ROUND(AVG(arr_delay),  2)                AS avg_arr_delay,
        ROUND(SUM(COALESCE(cancelled, 0)) / COUNT(*), 4) AS cancel_rate
    FROM flights_clean
    WHERE op_unique_carrier IS NOT NULL
      AND origin            IS NOT NULL
      AND month             IS NOT NULL
    GROUP BY op_unique_carrier, origin, month
),
active_months AS (
    SELECT
        op_unique_carrier,
        origin,
        -- FIX: COLLECT_SET su INT → SORT_ARRAY ordina numericamente (1,2,...,12)
        SORT_ARRAY(COLLECT_SET(month))          AS months_active
    FROM flights_clean
    WHERE op_unique_carrier IS NOT NULL
      AND origin            IS NOT NULL
    GROUP BY op_unique_carrier, origin
)
SELECT
    m.op_unique_carrier     AS carrier,
    m.origin,
    m.month,
    m.num_flights,
    m.min_arr_delay,
    m.max_arr_delay,
    m.avg_arr_delay,
    m.cancel_rate,
    a.months_active
FROM monthly_stats  m
JOIN active_months  a
  ON  m.op_unique_carrier = a.op_unique_carrier
  AND m.origin            = a.origin;

SELECT * FROM results_airline_stats
ORDER BY carrier, origin, month
LIMIT 10;