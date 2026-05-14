# Wikimedia Real-Time Streaming Analytics

Implemented by Rustem & Thiha

## Project Overview

This project ingests live Wikimedia recent change events, publishes them to Kafka, processes them using Spark Structured Streaming, stores aggregated results in HDFS as Parquet with Hive integration, and visualizes the analytics through a Streamlit dashboard.

The pipeline performs real-time aggregation of Wikimedia events by time window, wiki, event type, and bot flag. In addition, the project includes a bonus enrichment pipeline that joins live streaming data with a static reference dataset stored in HDFS.

## Architecture

Main pipeline:

Wikimedia Stream -> Kafka Producer -> Kafka Topic -> Spark Structured Streaming -> HDFS Parquet -> Hive External Table -> Streamlit Dashboard

Bonus pipeline:

Static Wiki Reference CSV in HDFS -> Spark Join with Live Streaming Data -> Enriched Parquet Output

## Technologies Used

- Python
- Kafka
- Spark Structured Streaming
- Spark SQL
- HDFS
- Hive
- Streamlit
- Docker
- Docker Compose

## Features

- Live ingestion of Wikimedia recent change events
- Kafka producer for streaming event delivery
- Spark Structured Streaming consumer
- Real-time windowed aggregation
- Persistent storage in HDFS Parquet
- Hive external table for querying processed data
- Streamlit dashboard for visualization
- Bonus enrichment using static reference data stored in HDFS

## Repository Structure

```text
producer/        Kafka producer for Wikimedia event stream
streaming/       Spark Structured Streaming jobs
dashboard/       Streamlit dashboard application
storage/         Hive schema definitions
static_data/     Static reference dataset for enrichment bonus
README.md        Project documentation

Data Source

The source of streaming data is the Wikimedia recent changes event stream.

Main Aggregation Logic

The main streaming job aggregates events using:

* 1-minute event time windows
* grouping by:
    * wiki
    * type
    * bot

The main aggregated output contains:

* window_start
* window_end
* wiki
* type
* bot
* count

Bonus Enrichment

For the bonus implementation, a static reference dataset was created and stored in HDFS:

* static_data/wiki_reference.csv

This dataset maps wiki values to additional metadata such as:

* wiki_group
* language_family
* region

The bonus Spark streaming job joins live streaming data with this static HDFS dataset and writes enriched output to a separate HDFS Parquet path.

Example enriched columns:

* wiki
* wiki_group
* language_family
* region
* type
* bot
* count

Prerequisites

Before running the project, make sure you have:

* Docker installed
* Docker Compose installed
* Python 3 installed on the host machine
* The provided lab environment available through Docker
* Streamlit installed in the dashboard virtual environment

Execution Steps

1. Start the Docker environment
docker compose up -d
docker ps

Make sure the required containers are running, including:

* cs523bdt-lab
* zookeeper-server
* kafka-server
* hive-metastore-db

2. Enter the lab container
docker exec -it cs523bdt-lab bash

3. Verify Hadoop services
Inside the lab container:
jps
hdfs dfsadmin -report

You should see services such as:

* NameNode
* DataNode
* SecondaryNameNode
* ResourceManager
* NodeManager

4. Create the Kafka topic

From the host machine:
docker exec -it kafka-server kafka-topics --bootstrap-server kafka-server:9092 --create --topic wikimedia-events --partitions 1 --replication-factor 1
docker exec -it kafka-server kafka-topics --bootstrap-server kafka-server:9092 --list

5. Run the Kafka producer

Inside the lab container:
cd /opt/my_code/final-project/producer
source venv/bin/activate
python wikimedia_producer.py

You should start seeing messages such as:
Sent 10 events to Kafka topic 'wikimedia-events'

6. Run the main streaming job

Open another terminal into the lab container and run:
cd /opt/my_code/final-project/streaming
spark-submit --master "local[2]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 wikimedia_streaming.py

7. Verify HDFS output

Inside the lab container:
hdfs dfs -ls /final_project/wikimedia/event_counts
hdfs dfs -du -h /final_project/wikimedia/event_counts

8. Create Hive database and table

Hive schema is stored in:
storage/hive_schema.sql

You can create the schema with:
hive -f /opt/my_code/final-project/storage/hive_schema.sql

9. Create Hive external table for main output

Inside the lab container:
hive -e "
DROP TABLE IF EXISTS wikimedia_analytics.event_counts_external;

CREATE EXTERNAL TABLE wikimedia_analytics.event_counts_external (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  wiki STRING,
  type STRING,
  bot BOOLEAN,
  count BIGINT
)
STORED AS PARQUET
LOCATION '/final_project/wikimedia/event_counts';
"

10. Run the Streamlit dashboard

On the host machine:
cd "/Users/macbook-pro/Desktop/FILES/MIU Files/Big Data Technology - DE course 2/Labs/projects/cs523-bdt/my_code/final-project/dashboard"
source venv/bin/activate
streamlit run app.py

Then open the local Streamlit URL in your browser.

Bonus Execution Steps

1. Upload static reference data to HDFS

Inside the lab container:
hdfs dfs -mkdir -p /final_project/static
hdfs dfs -put -f /opt/my_code/final-project/static_data/wiki_reference.csv /final_project/static/
hdfs dfs -ls /final_project/static
hdfs dfs -cat /final_project/static/wiki_reference.csv

2. Run the bonus streaming job

Inside the lab container:
cd /opt/my_code/final-project/streaming
spark-submit --master "local[2]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 wikimedia_streaming_bonus.py

3. Verify enriched output

Inside the lab container:
hdfs dfs -du -h /final_project/wikimedia/event_counts_enriched

Then query it directly with Spark SQL:
spark-sql -e "
SELECT COUNT(*)
FROM parquet.\`hdfs://localhost:9000/final_project/wikimedia/event_counts_enriched\`;
"

And sample enriched rows:

spark-sql -e "
SELECT wiki, wiki_group, language_family, region, type, bot, count
FROM parquet.\`hdfs://localhost:9000/final_project/wikimedia/event_counts_enriched\`
LIMIT 20;
"

Dashboard

The Streamlit dashboard provides:

* processed row metrics
* latest aggregated rows
* top wikis by event count
* bot vs human activity
* event type distribution

The dashboard uses caching to reduce repeated Hive query load.

Example Verification Commands

To check Kafka topic
docker exec -it kafka-server kafka-topics --bootstrap-server kafka-server:9092 --list

To check main parquet output with Spark SQL
spark-sql -e "
SELECT COUNT(*)
FROM parquet.\`hdfs://localhost:9000/final_project/wikimedia/event_counts\`;
"

To check bonus enriched parquet output with Spark SQL
spark-sql -e "
SELECT wiki, wiki_group, language_family, region, type, bot, count
FROM parquet.\`hdfs://localhost:9000/final_project/wikimedia/event_counts_enriched\`
LIMIT 20;
"

This project includes:

* Kafka producer scripts
* Spark streaming application code
* Hive schema definitions
* Streamlit visualization code
* static reference dataset for bonus enrichment
* Project files
* README with execution steps