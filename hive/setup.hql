-- ============================================================
-- SETUP HIVE: Database, tabella raw (STRING) + view tipizzata
-- ============================================================

CREATE DATABASE IF NOT EXISTS flights_db
COMMENT 'Database per analisi Flight Delay Dataset 2024';

USE flights_db;

-- ── Tabella grezza: OpenCSVSerde richiede TUTTE le colonne STRING ──
DROP TABLE IF EXISTS flights_raw;

CREATE EXTERNAL TABLE flights_raw (
    year                  STRING,
    month                 STRING,
    day_of_month          STRING,
    day_of_week           STRING,
    fl_date               STRING,
    op_unique_carrier     STRING,
    origin                STRING,
    origin_city_name      STRING,
    dest                  STRING,
    dest_city_name        STRING,
    dep_delay             STRING,
    arr_delay             STRING,
    cancelled             STRING,
    cancellation_code     STRING,
    diverted              STRING,
    distance              STRING,
    carrier_delay         STRING,
    weather_delay         STRING,
    nas_delay             STRING,
    security_delay        STRING,
    late_aircraft_delay   STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/diego/flights/clean'
TBLPROPERTIES ("skip.header.line.count"="1");

-- ── View tipizzata: CAST ai tipi corretti per le analisi ──
DROP TABLE IF EXISTS flights_clean;
DROP VIEW IF EXISTS flights_clean;

CREATE VIEW flights_clean AS
SELECT
    CAST(year             AS INT)    AS year,
    CAST(month            AS INT)    AS month,
    CAST(day_of_month     AS INT)    AS day_of_month,
    CAST(day_of_week      AS INT)    AS day_of_week,
    fl_date,
    op_unique_carrier,
    origin,
    origin_city_name,
    dest,
    dest_city_name,
    CAST(dep_delay        AS FLOAT)  AS dep_delay,
    CAST(arr_delay        AS FLOAT)  AS arr_delay,
    CAST(cancelled        AS INT)    AS cancelled,
    cancellation_code,
    CAST(diverted         AS INT)    AS diverted,
    CAST(distance         AS FLOAT)  AS distance,
    CAST(carrier_delay    AS FLOAT)  AS carrier_delay,
    CAST(weather_delay    AS FLOAT)  AS weather_delay,
    CAST(nas_delay        AS FLOAT)  AS nas_delay,
    CAST(security_delay   AS FLOAT)  AS security_delay,
    CAST(late_aircraft_delay AS FLOAT) AS late_aircraft_delay
FROM flights_raw;

-- ── Verifica ──
SELECT COUNT(*) AS total_records FROM flights_clean;

SELECT
    op_unique_carrier,
    origin,
    dep_delay,
    arr_delay,
    cancelled,
    month
FROM flights_clean
LIMIT 5;