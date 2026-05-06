#!/usr/bin/env python3
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def convert_csv_to_parquet():
    spark = SparkSession.builder \
        .appName("CSV to Parquet Converter") \
        .master("local[*]") \
        .getOrCreate()

    # Schema esatto del dataset cleaned
    schema = StructType([
        StructField("fl_date", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("op_unique_carrier", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("dest", StringType(), True),
        StructField("dep_delay", DoubleType(), True),
        StructField("arr_delay", DoubleType(), True),
        StructField("cancelled", DoubleType(), True),
        StructField("cancellation_code", StringType(), True),
        StructField("carrier_delay", DoubleType(), True),
        StructField("weather_delay", DoubleType(), True),
        StructField("nas_delay", DoubleType(), True),
        StructField("security_delay", DoubleType(), True),
        StructField("late_aircraft_delay", DoubleType(), True)
    ])

    base_dir = "data"
    subdirs = ["cleaned", "samples"]

    for subdir in subdirs:
        search_path = os.path.join(base_dir, subdir)
        if not os.path.exists(search_path):
            continue

        print(f"\n--- Elaborazione directory: {search_path} ---")
        
        for file in os.listdir(search_path):
            if file.endswith(".csv"):
                csv_path = os.path.join(search_path, file)
                # Sostituisce .csv con .parquet
                parquet_path = csv_path.replace(".csv", ".parquet")
                
                print(f"Convertendo {csv_path} in {parquet_path}...")
                
                try:
                    df = spark.read.csv(csv_path, header=True, schema=schema)
                    
                    # Scriviamo in formato Parquet (sovrascrivendo se esiste)
                    # Usiamo coalesce(1) se vogliamo un singolo file, ma Parquet 
                    # di solito preferisce più partizioni. Per i sample piccoli coalesce(1) è OK.
                    df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
                    
                    # Rinominiamo il file all'interno della cartella parquet per comodità (opzionale)
                    # Spark scrive una directory .parquet. Per semplicità qui la lasciamo così
                    # perché Spark/Hive leggono le directory parquet come file singoli.
                    
                    print(f"✅ Successo: {parquet_path}")
                except Exception as e:
                    print(f"❌ Errore durante la conversione di {file}: {e}")

    spark.stop()

if __name__ == "__main__":
    convert_csv_to_parquet()
