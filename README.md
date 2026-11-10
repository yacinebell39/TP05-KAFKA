# TP05: Real-Time Data Streaming and Processing Using Apache Kafka
### Comprehensive Implementation & Lab Execution Guide

[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9.1-red.svg?logo=apachekafka)](https://kafka.apache.org/)
[![Mode](https://img.shields.io/badge/Mode-KRaft%20(No%20ZooKeeper)-blue.svg)]()
[![Python](https://img.shields.io/badge/Python-3.10%2B-brightgreen.svg?logo=python)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker)](https://docs.docker.com/compose/)
[![Status](https://img.shields.io/badge/Lab%20Status-Completed%20%26%20Validated-success.svg)]()

**Institution:** University of Eloued  
**Faculty:** Faculty of Exact Sciences — Department of Computer Science  
**Program:** Master 2 in Artificial Intelligence & Data Science  
**Module:** Big Data Analytics (Academic Year: 2026/2027)  
**Deliverables:** [Academic Report (REPORT.md)](REPORT.md) | [Visual Evidence (screenshots/)](screenshots/README.md) | [PDF Report](REPORT.pdf)

---

## Table of Contents
1. [Overview & Architecture](#1-overview--architecture)
2. [Project Structure](#2-project-structure)
3. [Prerequisites & Environment Setup](#3-prerequisites--environment-setup)
4. [Baseline Execution Guide (Tasks 1 to 5)](#4-baseline-execution-guide-tasks-1-to-5)
   - [Step 1: Deploy Kafka with Docker Compose (Task 1)](#step-1-deploy-kafka-with-docker-compose-task-1)
   - [Step 2: Verify Python Client Environment (Task 2)](#step-2-verify-python-client-environment-task-2)
   - [Step 3: Create & Inspect Kafka Topic (Task 5)](#step-3-create--inspect-kafka-topic-task-5)
   - [Step 4: Launch Consumer Stream Monitor (Task 4)](#step-4-launch-consumer-stream-monitor-task-4)
   - [Step 5: Run Producer Event Generator (Task 3)](#step-5-run-producer-event-generator-task-3)
   - [Step 6: Inspect Consumer Groups & Offsets (Task 5)](#step-6-inspect-consumer-groups--offsets-task-5)
5. [Advanced Experiments Guide (Further Investigation)](#5-advanced-experiments-guide-further-investigation)
   - [Experiment 1: High-Throughput Stream (100 Events)](#experiment-1-high-throughput-stream-100-events)
   - [Experiment 2: Cumulative Running Average Analytics](#experiment-2-cumulative-running-average-analytics)
   - [Experiment 3: Independent vs Shared Consumer Groups](#experiment-3-independent-vs-shared-consumer-groups)
   - [Experiment 4: Partition Dynamic Scaling (1 to 3 Partitions)](#experiment-4-partition-dynamic-scaling-1-to-3-partitions)
   - [Experiment 5: Campus Building Tagging & Stream Filtering](#experiment-5-campus-building-tagging--stream-filtering)
   - [Experiment 6: Data Persistence & CSV Export (Optional)](#experiment-6-data-persistence--csv-export-optional)
6. [Data Schema & Protocol](#6-data-schema--protocol)
7. [Cleanup & Teardown](#7-cleanup--teardown)
8. [Frequently Asked Questions (FAQ)](#8-frequently-asked-questions-faq)

---

## 1. Overview & Architecture

This lab implements an end-to-end, real-time Internet of Things (IoT) stream processing pipeline for smart campus environmental monitoring. Simulated sensors publish continuous temperature and relative humidity metrics to an Apache Kafka cluster running in **KRaft** (Kafka Raft Metadata) mode without ZooKeeper. Downstream consumers deserialize JSON streams, detect temperature anomalies ($T \ge 35^\circ\text{C}$), compute streaming aggregates, and balance workloads across partitions.

```
       +-------------------------------------------------------------+
       |                 Producer (producer.py)                     |
       |  Generates JSON Sensor Events (T, RH, timestamp, sensor_id) |
       +------------------------------+------------------------------+
                                      |
                                      | PLAINTEXT :9092 (acks="all")
                                      v
       +-------------------------------------------------------------+
       |            Apache Kafka Broker (KRaft Mode)                 |
       |              Container: tp05-kafka (:9092)                  |
       |  +-------------------------------------------------------+  |
       |  | Topic: campus-sensors (Replication Factor = 1)        |  |
       |  | [ Partition 0 ]    [ Partition 1 ]    [ Partition 2 ] |  |
       |  +-------------------------------------------------------+  |
       +------------------------------+------------------------------+
                                      |
                                      | auto_offset_reset="earliest"
                                      v
       +-------------------------------------------------------------+
       |                 Consumer (consumer.py)                     |
       |  - JSON Deserialization & Metric Parsing                    |
       |  - Real-Time Anomaly Alerting (T >= 35 C)                   |
       |  - Stateful Running Average Analytics                       |
       |  - Consumer Group Load Balancing & Partition Offsets        |
       |  - Stream Filtering & CSV Data Persistence                  |
       +-------------------------------------------------------------+
```

---

## 2. Project Structure

```
tp05-kafka/
├── compose.yaml          # Kafka KRaft single-broker container orchestration
├── requirements.txt      # Python dependency specification (kafka-python)
├── producer.py           # Configurable stream generator with CLI flags
├── consumer.py           # Stream processor with alerting, stats & filtering
├── README.md             # This comprehensive lab guide
├── REPORT.md             # Theoretical questions, answers & experimental logs
├── REPORT.pdf            # Formal printable academic report
├── .gitignore            # Clean git exclusion rules
└── screenshots/          # 11 authentic terminal execution captures
    ├── 01_compose_status.png
    ├── 02_topic_description.png
    ├── 03_producer_30.png
    ├── 04_consumer_30.png
    ├── 05_experiment_100_events.png
    ├── 06_experiment_running_average.png
    ├── 07_experiment_independent_groups.png
    ├── 08_experiment_shared_group.png
    ├── 09_experiment_partitions.png
    ├── 10_experiment_building_filter.png
    ├── 11_experiment_csv_export.png
    └── README.md
```

---

## 3. Prerequisites & Environment Setup

* **Docker Desktop** (Windows/macOS) or **Docker Engine & Docker Compose** (Linux).
* **Python 3.10+** and **pip**.
* Three terminal windows (PowerShell on Windows, or Bash on Linux/macOS):
  * **Terminal A:** Kafka administration & Producer.
  * **Terminal B:** Primary Consumer.
  * **Terminal C:** Secondary Consumer (for concurrency and group testing).

### Virtual Environment Setup
From the project root directory (`D:\University\tp05-kafka`):

```powershell
# 1. Create a virtual environment
python -m venv .venv

# 2. Activate virtual environment (Windows PowerShell)
.\.venv\Scripts\Activate.ps1
# On Linux/macOS: source .venv/bin/activate

# 3. Install required dependencies
python -m pip install -r requirements.txt
```

---

## 4. Baseline Execution Guide (Tasks 1 to 5)

### Step 1: Deploy Kafka with Docker Compose (Task 1)
In **Terminal A**, launch the Kafka single broker in KRaft mode:

```powershell
docker compose up -d
docker compose ps
docker compose logs --tail=30 kafka
```
*Expected Result:* `tp05-kafka` is running, exposing `0.0.0.0:9092->9092/tcp`, with logs confirming transition to `RUNNING` and `PLAINTEXT endpoint READY`.

### Step 2: Verify Python Client Environment (Task 2)
```powershell
.\.venv\Scripts\python.exe -m pip show kafka-python
```

### Step 3: Create & Inspect Kafka Topic (Task 5)
Create topic `exam01-basic` with 3 partitions and a replication factor of 1:

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic exam01-basic --partitions 3 --replication-factor 1
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic exam01-basic
```
*Expected Result:* Topic metadata confirms `PartitionCount: 3`, `ReplicationFactor: 1`, with leader broker `1` for partitions 0, 1, and 2.

### Step 4: Launch Consumer Stream Monitor (Task 4)
In **Terminal B**, start the consumer before the producer:

```powershell
.\.venv\Scripts\python.exe consumer.py --topic exam01-basic --group-id exam01-basic-group
```
*Expected Result:* Consumer listens to topic `exam01-basic` under consumer group `exam01-basic-group`.

### Step 5: Run Producer Event Generator (Task 3)
In **Terminal A**, publish 30 simulated readings (1 event per second by default):

```powershell
.\.venv\Scripts\python.exe producer.py --topic exam01-basic --count 30 --seed 5
```
*Observations:*
* **Terminal A (Producer):** Outputs JSON payloads with broker acknowledgements (`partition=... offset=...`).
* **Terminal B (Consumer):** Deserializes messages, prints formatted metrics, and flags high temperatures:
  `ALERT: High temperature detected!` whenever $T \ge 35^\circ\text{C}$.

### Step 6: Inspect Consumer Groups & Offsets (Task 5)
In **Terminal A** (while Consumer B is active):

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker exec tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group exam01-basic-group
```
*Observations:* Demonstrates partition assignment, current offset, log end offset, and zero consumer lag (`LAG = 0`). Stop Consumer B with `Ctrl+C`.

---

## 5. Advanced Experiments Guide (Further Investigation)

### Experiment 1: High-Throughput Stream (100 Events)
Tests buffer durability, message sequencing, and consumer processing under a larger payload.

* **Terminal A (Create Topic):**
  ```powershell
  docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic exam01-100 --partitions 3 --replication-factor 1
  ```
* **Terminal B (Consumer):**
  ```powershell
  .\.venv\Scripts\python.exe consumer.py --topic exam01-100 --group-id exam01-100-group --max-messages 100
  ```
* **Terminal A (Producer):**
  ```powershell
  .\.venv\Scripts\python.exe producer.py --topic exam01-100 --count 100 --seed 5
  ```
*Outcome:* Consumer automatically stops after receiving exactly 100 events (`received=100, displayed=100`).

---

### Experiment 2: Cumulative Running Average Analytics
Demonstrates real-time stateful computation over continuous streaming data.

* **Terminal A (Create Topic):**
  ```powershell
  docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic exam01-average --partitions 3 --replication-factor 1
  ```
* **Terminal B (Consumer with Running Average):**
  ```powershell
  .\.venv\Scripts\python.exe consumer.py --topic exam01-average --group-id exam01-average-group --running-average --max-messages 30
  ```
* **Terminal A (Producer):**
  ```powershell
  .\.venv\Scripts\python.exe producer.py --topic exam01-average --count 30 --seed 5
  ```
*Outcome:* Consumer calculates $\bar{T}_n = \frac{1}{n}\sum T_i$ incrementally on each arriving message and reports final aggregate average.

---

### Experiment 3: Independent vs Shared Consumer Groups

#### Case 1: Two Consumers in Distinct Groups (Broadcast / Pub-Sub)
Each group manages independent partition offsets and receives the entire stream.

* **Terminal B (Group A):**
  ```powershell
  .\.venv\Scripts\python.exe consumer.py --topic exam01-independent --group-id exam01-group-A --max-messages 30
  ```
* **Terminal C (Group B):**
  ```powershell
  .\.venv\Scripts\python.exe consumer.py --topic exam01-independent --group-id exam01-group-B --max-messages 30
  ```
* **Terminal A (Producer):**
  ```powershell
  docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic exam01-independent --partitions 3 --replication-factor 1
  .\.venv\Scripts\python.exe producer.py --topic exam01-independent --count 30 --seed 5
  ```
*Outcome:* Both Consumer B and Consumer C receive all 30 events (`30/30` each).

#### Case 2: Two Consumers in the Same Group (Load Balancing / Point-to-Point)
Partitions are divided across members; each message is processed once by the group.

* **Terminal B & Terminal C (Same Group):**
  ```powershell
  .\.venv\Scripts\python.exe consumer.py --topic exam01-shared --group-id exam01-shared-group
  ```
* **Terminal A (Producer & Describe):**
  ```powershell
  docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic exam01-shared --partitions 3 --replication-factor 1
  .\.venv\Scripts\python.exe producer.py --topic exam01-shared --count 30 --seed 5
  docker exec tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group exam01-shared-group
  ```
*Outcome:* The 3 partitions and 30 messages are split between Consumer B and Consumer C.

---

### Experiment 4: Partition Dynamic Scaling (1 to 3 Partitions)
Demonstrates concurrency boundaries and dynamic topic alteration in Kafka.

1. **Phase 1: 1 Partition Topic**
   ```powershell
   docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic exam01-partitions --partitions 1 --replication-factor 1
   ```
   Start two consumers in `exam01-partitions-group` (Terminal B & C) and publish 30 events from Terminal A.  
   *Inspection:* Only one consumer is assigned partition 0; the second consumer is idle.

2. **Phase 2: Scale Topic to 3 Partitions**
   ```powershell
   docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic exam01-partitions --partitions 3
   ```
   Restart consumers and publish a second batch of 30 events.  
   *Inspection:* Kafka rebalances partitions across both active consumers.

---

### Experiment 5: Campus Building Tagging & Stream Filtering
Enriches data payloads with categorical metadata (`campus_building`) and filters client-side.

* **Terminal B (Consumer with Filter):**
  ```powershell
  .\.venv\Scripts\python.exe consumer.py --topic exam01-buildings --group-id exam01-building-A --building building-A --max-messages 30
  ```
* **Terminal A (Producer with Building Metadata):**
  ```powershell
  docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic exam01-buildings --partitions 3 --replication-factor 1
  .\.venv\Scripts\python.exe producer.py --topic exam01-buildings --count 30 --include-building --seed 5
  ```
*Outcome:* Consumer receives all 30 events but displays and counts only `building-A` records (`received=30, displayed=6`).

---

### Experiment 6: Data Persistence & CSV Export (Optional)
Saves consumed streaming messages into a persistent CSV structured format.

* **Terminal B (Consumer with CSV Export):**
  ```powershell
  .\.venv\Scripts\python.exe consumer.py --topic exam01-csv --group-id exam01-csv-group --csv data/exam01-events.csv --max-messages 30
  ```
* **Terminal A (Producer & Verification):**
  ```powershell
  docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic exam01-csv --partitions 3 --replication-factor 1
  .\.venv\Scripts\python.exe producer.py --topic exam01-csv --count 30 --seed 5
  
  # Inspect generated CSV header & records
  Get-Content .\data\exam01-events.csv -TotalCount 6
  (Import-Csv .\data\exam01-events.csv | Measure-Object).Count
  ```
*Outcome:* Creates `data/exam01-events.csv` containing 30 records with the required schema (`event_id`, `sensor_id`, `temperature_c`, `humidity_pct`, `timestamp`).

---

## 6. Data Schema & Protocol

Each generated sensor message adheres to the following JSON structure:

```json
{
  "event_id": 1,
  "sensor_id": "sensor-02",
  "temperature_c": 36.42,
  "humidity_pct": 58.17,
  "timestamp": "2026-11-10T21:39:43.123456+00:00",
  "campus_building": "building-B"
}
```

* `event_id`: Integer sequence identifier (1 to $N$).
* `sensor_id`: Categorical device ID (`sensor-01` to `sensor-04`).
* `temperature_c`: Simulated ambient temperature ($18.0^\circ\text{C}$ to $42.0^\circ\text{C}$).
* `humidity_pct`: Relative humidity percentage ($25.0\%$ to $85.0\%$).
* `timestamp`: High-precision UTC ISO-8601 generation timestamp.
* `campus_building`: Optional architectural zone attribute (`building-A` to `building-D`).

---

## 7. Cleanup & Teardown

To stop the Kafka service while preserving volume state:
```powershell
docker compose down
```

To completely delete the Kafka broker container and persistent volume data:
```powershell
docker compose down -v
```

---

## 8. Frequently Asked Questions (FAQ)

**Q: Why KRaft mode instead of ZooKeeper?**  
A: As of Apache Kafka 3.x, KRaft is the modern, official consensus protocol. It simplifies operations by eliminating the external ZooKeeper cluster, running metadata management natively inside Kafka brokers.

**Q: What is the significance of `auto_offset_reset='earliest'`?**  
A: It ensures that when a new consumer group joins with no previous committed offsets, it reads all available historical messages from the beginning of the topic log rather than only receiving future messages.

---
*Developed for University of Eloued — Master 2 IA & Data Science.*
