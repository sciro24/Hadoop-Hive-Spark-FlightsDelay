# Flight Delay 2024 — Big Data Analysis

Comparative analysis of **MapReduce**, **Apache Hive**, **Spark Core** and **Spark SQL** on the [Flight Delay Dataset 2024](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024) (~7M rows).  
Executed both **locally** (single machine) and on **AWS EMR cluster** (1 master + 3 workers m5.xlarge).

> Università degli Studi Roma Tre — Corso di Big Data — A.A. 2025/2026

---

## Table of Contents

- [Project Structure](#project-structure)
- [Analyses](#analyses)
- [Technologies](#technologies)
- [Quick Start — Local](#quick-start--local)
- [Quick Start — AWS EMR Cluster](#quick-start--aws-emr-cluster)
- [Benchmark Suite](#benchmark-suite)
- [Results](#results)
- [Requirements](#requirements)

---

## Project Structure

```
Hadoop-Hive-Spark-FlightsDelay/
│
├── data_preparation/              
│   ├── cleaning.py                
│   ├── convert_to_parquet.py       
│   └── generate_samples.py        
│
├── analysis_1_airline_stats/       
│   ├── mapreduce/
│   │   ├── mapper.py               
│   │   ├── reducer.py              
│   │   └── run.sh                  
│   ├── hive/
│   │   ├── queries.hql             
│   │   └── run.sh
│   └── spark_sql/
│       ├── job.py                  
│       └── run.sh
│
├── analysis_2_delay_report/        
│   ├── hive/
│   │   ├── queries.hql            
│   │   └── run.sh
│   ├── spark_core/
│   │   ├── job.py                  
│   │   └── run.sh
│   └── spark_sql/
│       ├── job.py                  
│       └── run.sh
│
├── analysis_3_ranking/             
│   ├── mapreduce/
│   │   ├── mapper.py               
│   │   ├── reducer.py             
│   │   └── run.sh
│   ├── spark_core/
│   │   ├── job.py                  
│   │   └── run.sh
│   └── spark_sql/
│       ├── job.py                 
│       └── run.sh
│
├── aws/                            
│   ├── config.sh                  
│   ├── create_cluster.sh           
│   ├── upload_data.sh              
│   ├── run_benchmarks_cluster.sh   
│   ├── rerun_mapreduce_cluster.sh 
│   └── collect_results.sh         
│
├── benchmarks/                    
│   ├── benchmark_tracker.py        
│   ├── run_benchmarks.sh           
│   ├── collect_samples.py          
│   ├── results_local.csv           
│   ├── results_cluster.csv        
│   ├── analisi_benchmark_local.ipynb    
│   ├── analisi_benchmark_cluster.ipynb 
│   ├── confronto_locale_cluster.ipynb   
│   ├── plots_local/              
│   ├── plots_cluster/              
│   └── plots_local_vs_cluster/    
│
├── data/                           
│   ├── raw/                        # Original Kaggle CSV (~1.1 GB)
│   ├── cleaned/                    # Cleaned CSV (354 MB) + Parquet (43 MB)
│   └── samples/                    # Benchmark samples (10/25/50/125/150%)
│
├── eda/                            
│   ├── eda_pre_cleaning.ipynb      # EDA on raw dataset
│   ├── eda_post_cleaning.ipynb     # EDA after cleaning
│   └── plots/                      
│
├── results/                        
│   ├── analysis_1/
│   ├── analysis_2/
│   └── analysis_3/
│                   
│                
├── requirements.txt                # Python dependencies
└── .gitignore
```

---

## Analyses

### 3.1 — Airline Statistics
For each `(carrier, origin, month)` tuple: number of flights, min/max/avg arrival delay, cancellation rate, list of active months.

### 3.2 — Delay Report by Airport and Period
For each `(origin, month)`: flights split into **low** (<15 min), **medium** (15–60 min) and **high** (>60 min) departure delay bands, with average dep/arr delay per band and top-3 delay causes.

### 3.3 — Carrier–Airport Anomaly Ranking
For each `(origin, carrier)`: performance metrics compared against the airport average — dep/arr delay, cancellation rate, delta from airport mean, and rank (best to worst) within the airport.

---

## Technologies

| Technology | Version | Input format |
| :--- | :--- | :--- |
| MapReduce | Hadoop 3.4.1 (Streaming) | CSV |
| Apache Hive | 2.3.9 | Parquet |
| Spark Core | 3.5.8 | Parquet |
| Spark SQL | 3.5.8 | Parquet |

All jobs support both **local** and **cluster** mode via the `CLUSTER_MODE=true` environment variable. Output is routed to S3 automatically when running on cluster.

---

## Quick Start — Local

### 1. Prerequisites

- Python 3.10+
- Apache Hadoop 
- Apache Hive 3.4.1
- Apache Spark 3.5.8
- Java 11+

```bash
# Clone the repository
git clone https://github.com/sciro24/Hadoop-Hive-Spark-FlightsDelay.git
cd Hadoop-Hive-Spark-FlightsDelay

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Dataset

Download the dataset from Kaggle and place it at `data/raw/flight_data_2024.csv`:

```
https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024
```

### 3. Data Preparation

```bash
# Step 1 — Clean the raw dataset (outputs data/cleaned/flight_data_2024_cleaned.csv)
python3 data_preparation/cleaning.py

# Step 2 — Convert to Parquet (outputs data/cleaned/flight_data_2024_cleaned.parquet)
python3 data_preparation/convert_to_parquet.py

# Step 3 — Generate benchmark samples (10/25/50/125/150% of original)
python3 data_preparation/generate_samples.py
```


### 4. Run the Full Benchmark Suite

```bash
# Runs all 54 jobs (9 combinations × 6 dataset sizes) and writes results_local.csv
bash benchmarks/run_benchmarks.sh
```

Progress is printed to stdout. The benchmark tracker records timestamp, elapsed time, exit code and input metadata for each job in `benchmarks/results_local.csv`.

### 5. Analyze Results

Open the Jupyter notebooks from the `benchmarks/` directory:

```bash
cd benchmarks
jupyter lab
```

| Notebook | Description |
|---|---|
| `analisi_benchmark_local.ipynb` | Charts and tables for local benchmark results |
| `analisi_benchmark_cluster.ipynb` | Charts and tables for cluster benchmark results |
| `confronto_locale_cluster.ipynb` | Side-by-side local vs cluster comparison |

Charts are saved automatically to `plots_local/`, `plots_cluster/`, and `plots_local_vs_cluster/`.

---

## Quick Start — AWS EMR Cluster

### Prerequisites

- AWS CLI configured with valid credentials (AWS Academy: renew every ~4 hours)
- An EC2 Key Pair in your region (`.pem` file)
- Data already uploaded to S3 (or run `upload_data.sh`)

### 1. Configure

Edit `aws/config.sh` and set your S3 bucket name and key pair:

```bash
S3_BUCKET="your-bucket-name"
KEY_NAME="your-key-pair-name"
```

### 2. Upload Data and Scripts to S3

```bash
bash aws/upload_data.sh
```

### 3. Create the Cluster

```bash
bash aws/create_cluster.sh

After cluster creation, open port 22 in the EMR master Security Group:  
AWS Console → EC2 → Security Groups → `ElasticMapReduce-master` → Inbound rules → Add SSH rule
```

### 4. Run Benchmarks on the Cluster


# Run full benchmark suite
```
bash aws/run_benchmarks_cluster.sh
```


## Requirements

```
pandas>=2.0
pyarrow>=12.0
pyspark>=3.5
matplotlib>=3.7
numpy>=1.24
jupyter>=1.0
```

Install with:

```bash
pip install -r requirements.txt
```
