# Analisi Completa del Progetto: Hadoop-Hive-Spark-FlightsDelay

## 1. Panoramica del Progetto
Il progetto implementa una pipeline di analisi per ritardi aerei (dataset 2024) confrontando diverse tecnologie Big Data: **Hadoop MapReduce**, **Apache Hive**, **Spark Core (RDD)** e **Spark SQL**. 
Il sistema prevede tre tipologie di analisi (Analisi 1, 2 e 3) e un framework per effettuare benchmark sulle performance in locale e su cluster.

### Struttura e Moduli
- **`data_preparation/`**: Contiene gli script Python per la pulizia del dataset (`cleaning.py`) e per la generazione di sample per i test di scalabilità (`generate_samples.py`).
- **`analysis_1_airline_stats/`**: Statistiche descrittive (ritardi, voli, cancellazioni) raggruppate per compagnia aerea, aeroporto e mese.
- **`analysis_2_delay_report/`**: Report per fasce di ritardo e analisi delle 3 cause principali di ritardo per aeroporto/mese.
- **`analysis_3_ranking/`**: Ranking delle compagnie aeree per ciascun aeroporto in base alla differenza tra il proprio ritardo medio in partenza e la media globale dell'aeroporto.
- **`benchmarks/`**: Script Bash e Python per orchestrare l'esecuzione dei job, raccogliere i tempi di elaborazione e prelevare campioni degli output.

## 2. Analisi dell'Implementazione e Problematiche Rilevate

Di seguito un'analisi approfondita del codice con l'evidenza di alcuni **bug logici e architetturali critici** che compromettono i risultati su un cluster reale o introducono inefficienze.

### 🔴 Criticità Alta

#### A. Bug nel Partizionamento MapReduce (Analisi 3 - Ranking)
- **File**: `analysis_3_ranking/mapreduce/mapper.py` e `reducer.py`
- **Descrizione**: Il mapper emette due tipi di righe utilizzando chiavi testuali fisse (`carrier` e `airport`) come primo elemento della stringa separata da tab:
  - `carrier\t{origin}\t{carrier}...`
  - `airport\t{origin}\t__ALL__...`
- **Problema**: In Hadoop Streaming, per impostazione predefinita, la chiave di partizionamento è determinata dalla prima colonna (fino al primo tab). Di conseguenza, **tutte le righe `carrier` andranno a un Reducer e tutte le righe `airport` andranno a un altro Reducer**. Il Reducer che elabora i carrier avrà la variabile `airport_stats` completamente vuota (il calcolo `avg_airport` sarà `0.0`), portando a un calcolo errato di `dep_diff` (che verrà confrontato con 0 anziché con la media).
- **Correzione**: Il mapper deve emettere `origin` come primissima chiave (es. `origin\tcarrier\t...`), garantendo che tutti i record di un dato aeroporto vengano processati dalla stessa istanza del Reducer.

#### B. Errore Logico Query Hive (Analisi 1 - Airline Stats)
- **File**: `analysis_1_airline_stats/hive/queries.hql`
- **Descrizione**: La query calcola l'elenco dei mesi attivi per una compagnia utilizzando `COLLECT_SET(CAST(month AS STRING)) AS active_months`.
- **Problema**: Poiché la query effettua un raggruppamento per `(op_unique_carrier, origin, month)`, la funzione `COLLECT_SET(month)` opererà all'interno di un singolo mese. Il risultato sarà un array contenente sempre e solo un unico elemento (es. `["1"]`), fallendo l'obiettivo di trovare tutti i mesi operativi nell'arco dell'anno.
- **Correzione**: Implementare una CTE o una subquery (esattamente come fatto, correttamente, nello script Spark SQL) per calcolare i mesi attivi a livello aggregato di `carrier` e `origin` prima di effettuare la join col risultato finale mensile.

#### C. Rischio OutOfMemory (OOM) in MapReduce (Analisi 1)
- **File**: `analysis_1_airline_stats/mapreduce/reducer.py`
- **Descrizione**: Il Reducer salva tutti i ritardi di volo per ogni mese all'interno di una lista in RAM: `rec["delays"].append(float(arr_delay_str))`.
- **Problema**: Nel paradigma MapReduce, accumulare milioni di elementi (record) in una singola lista Python nel Reducer porta inevitabilmente a un esaurimento della memoria (OutOfMemoryError) quando processati volumi massivi.
- **Correzione**: Calcolare una "running stat". Poiché le uniche metriche richieste sono `min`, `max` e `avg`, il codice deve mantenere in memoria solo le variabili aggreganti: `count`, `sum`, `min` e `max`, aggiornandole riga per riga, evitando del tutto l'append ad una lista.

### 🟡 Criticità Media

#### D. Schema di Output Incoerente tra Tecnologie (Analisi 1)
- **Descrizione**: L'output non è omogeneo per l'Analisi 1 a seconda del motore utilizzato.
  - **Spark SQL**: Ritorna 9 colonne (include la colonna `months_active`).
  - **Hive**: Ritorna 9 colonne (include `active_months` calcolato male).
  - **MapReduce**: Ritorna solo 8 colonne (la logica dei mesi attivi è completamente assente nel file Python).
- **Correzione**: Allineare gli output, implementando nel `reducer.py` di MapReduce l'accumulazione dei mesi attivi, oppure rimuovere questo requisito da Spark e Hive.

#### E. Intestazioni (Header) Duplicate nei Sample del Benchmark
- **File**: `benchmarks/collect_samples.py`
- **Descrizione**: Lo script inietta un header personalizzato (`fout.write(header + "\n")`) prelevando poi le prime 10 righe dall'output nudo per comporre i campioni locali. 
- **Problema**: I job Spark SQL sono stati configurati per salvare i CSV *con l'header predefinito* (`.option("header", "true")`). Ciò porta i file `sample_top10.csv` derivati da Spark SQL ad avere due righe di intestazione di seguito (quella del python e quella di spark trattata per sbaglio come prima riga del dataset).
- **Correzione**: Verificare se la prima riga letta (dall'output Hadoop) è già un'intestazione per scartarla durante la raccolta dei sample.

#### F. Valutazione Silenziosa dei Ritardi (Analisi 2)
- **File**: `analysis_2_delay_report/spark_sql/job.py` (e `hive/queries.hql`)
- **Descrizione**: La logica SQL inserisce una fascia di default con `ELSE 'low'`. I valori NULL per il ritardo di partenza (`dep_delay`) vengono catalogati silenziosamente come `"low"`.
- **Correzione**: Risulterebbe concettualmente più pulito avere una fascia temporale `unknown` per i campi NULL o escludere preventivamente le righe in cui `dep_delay IS NULL` nei calcoli dei `delay_band`.

## 3. Punti di Forza dell'Architettura Esistente
- **Ottima modularità**: La separazione architetturale divide nettamente preparazione dati (`cleaning.py`), l'esecuzione scalabile in cluster (`job.py`, `run.sh`) e i check di affidabilità/scalabilità.
- **Codice Spark ottimizzato**: L'uso dell'API `RDD` applica pattern validi (come `.cache()` sui dati parsati) e gli script Spark SQL sfruttano efficientemente la registrazione delle View Temporanee e le Window Functions per rankizzare in modo nativo.
- **Automazione test**: L'approccio usato con `run_benchmarks.sh` unito a `benchmark_tracker.py` è una soluzione solida, elegante e completamente scalabile per testare variazioni su scala e salvare autonomamente il report locale in `results_local.csv`.
