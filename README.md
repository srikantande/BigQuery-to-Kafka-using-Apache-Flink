# BigQuery-to-Kafka-using-Apache-Flink
BigQuery to Kafka using Apache Flink

**What it does:** Reads your config → Connects BigQuery → Connects Kafka → Moves data automatically.

**No coding needed!** Edit `config.properties` + `schema.json` for any BigQuery table.

## BigQuery to Kafka - Apache Flink Pipeline

Move data from Google BigQuery to Apache Kafka using Apache Flink Table API. No coding required for new tables - just update a JSON file!

✅ Production-ready | ✅ Fully externalized config | ✅ Any BigQuery table | ✅ 12-Factor App compliant

🎥 What This Does

Reads any BigQuery table (batch or streaming)

Transforms data (type casting, null handling)

Writes to Kafka as JSON messages with string keys

Zero code changes for new tables - edit schema.json only

## 📋 Prerequisites
### ☁️ Google Cloud

1. GCP Service Account JSON key file
2. BigQuery table access (Reader role)

### 🐳 Kafka

1. Kafka broker (e.g., srilab.com:9092)
2. Target topic (e.g., flinkTopic_cdcdataagg)

### ⚡ Apache Flink 1.18

1. Ready Apache Flink 1.18 setup

## Explanation of pom.xml and App.java

### pom.xml

#### Java version:

It tells Maven to compile the code using Java 11 (<maven.compiler.source>11</maven.compiler.source>).

#### Dependencies:

These are the external libraries your app needs to work.
Flink streaming and table API for building streaming data apps.
Kafka connector to read/write data from Apache Kafka.
BigQuery connector to connect with Google BigQuery.
Jackson library for handling JSON data.
SLF4J for logging messages that help debug and monitor your app.

### App.java

#### Purpose

This is your Java application code that runs the data pipeline moving records from BigQuery to Kafka using Flink.

#### Reading Configs

It reads config.properties and schema.json, which are outside the app JAR, so you can easily change configuration without touching the code.

#### Main steps it performs

Loads configuration properties (like Kafka address, BigQuery credentials location, table names).
Loads a JSON file describing the BigQuery table's schema — which columns, their types, and how they map to the Kafka fields.
Sets up Flink’s streaming or batch environment based on your config.
Dynamically builds SQL commands (DDL and INSERT statements) based on the schema and config. This creates source and sink tables in Flink for BigQuery and Kafka.
Runs the pipeline that reads from BigQuery, applies type conversions and null safety, and writes to Kafka as JSON messages.

#### Flexibility & Maintainability

Because all table and connection details live outside the compiled code, you can reuse this app for different BigQuery tables with no code change. Just update config files and schema definition.

#### Logging

It logs helpful info about each major step to STDOUT/console for easy debugging and monitoring.

## config.properties

external-config/config.properties

## schema.json

external-config/schema.json

## Running Apache Flink Pipeline job

### Stage the required files

Copy and paste the following files e.g. path /opt/flink
1. gcp_serviceaccount_key.json
2. schema.json
3. config.properties 

**Update the config.properties with correct and absolute details for bigquery.credentials.path and schema.definition.path**

### Update the schema.json as per source BigQuery table DDL

Refering to the source BQ table DDL and destinaton Kafka messahe value rewrite the schema file accordingly. E.g:
```
  "columns": [
    {
      "name": "id",
      "sourceType": "STRING",
      "sinkType": "STRING",
      "nullable": false,
      "keyField": true
    },
    {
      "name": "vote_average",
      "sourceType": "STRING",
      "sinkType": "DOUBLE",
      "nullable": true,
      "keyField": false
    },
    {
      "name": "vote_count",
      "sourceType": "STRING",
      "sinkType": "BIGINT",
      "nullable": true,
      "keyField": false
    },
```
### Execution of pipeline
```
$ flink run /opt/flink/bigquery-to-kafka-flink-1.0-SNAPSHOT.jar /opt/flink/config.properties
```

