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

## Building JAR file
```
mvn clean package 
```
### Shipping the JAR, gcp_serviceaccount_key.json, schema.json, & config.properties

Ship the target/bigquery-to-kafka-flink-1.0-SNAPSHOT-shaded.jar gcp_serviceaccount_key.json, schema.json, & config.properties files to Apache Flink server

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

## Annexure

### schema.json built based on following DDL of Bigquery Table
```
  id INT64,
  imdb_id STRING,
  popularity STRING,
  vote_average STRING,
  vote_count STRING,
  imdb_rating STRING,
  imdb_votes STRING,
  title STRING,
  original_title STRING,
  tagline STRING,
  overview STRING,
  budget STRING,
  revenue STRING,
  runtime STRING,
  status STRING,
  release_date STRING,
  original_language STRING,
  `cast` STRING,
  director STRING,
  director_of_photography STRING,
  writers STRING,
  producers STRING,
  music_composer STRING,
  genres STRING,
  production_companies STRING,
  production_countries STRING,
  spoken_languages STRING,
  poster_path STRING,
  __op STRING
```

Sample select query JSON output of movie id 431150
```
[{
  "id": "431150",
  "imdb_id": "tt0068436",
  "popularity": "1.686",
  "vote_average": "2.0",
  "vote_count": "1.0",
  "imdb_rating": "4.8",
  "imdb_votes": "20.0",
  "title": "La curiosa",
  "original_title": "La curiosa",
  "tagline": "",
  "overview": "",
  "budget": "0.0",
  "revenue": "0.0",
  "runtime": "0.0",
  "status": "Released",
  "release_date": "1973-01-24",
  "original_language": "es",
  "cast": "Roberto Daniel, José Yepes, Rafaela Aparicio, Katy Vadillo, Isabel Pallarés, Ingrid Rabel, Verónica Llimerá, Vicente Roca, José Riesgo, Francisco Ortuño, Mirta Miller, Gustavo Casado, Guadalupe Muñoz Sampedro, Paloma Juanes, Valentina Gutiérrez, Ángel Picazo, Nené Morales, José Fernández, Rosita Fuster, Paco Lara, Cristino Almodóvar, Francisco Agudín, Fabián Conde, Rosa Fontana, Alfonso del Real, Antonio Cintado, E.T. Ruiz, Asunción Aranda, Esther Santana, Pilar Gómez Ferrer, María Isbert, Yolanda Ríos, Manuel de Blas, Carmen Martínez Sierra, Paquita Ruiz, Simón Arriaga, Liliane Meric, Luis Coromina, Máximo Valverde, Luis Barbero, Jimmy Arnau, Betsabé Ruiz, Mari Carmen Prendes, Mari Carmen Duque, Paca Gabaldón, Mery Leyva, Pepita Jiménez, Josele Román, Patty Shepard, Lola Tejela, Beni Deus, Pedro Valentín, José María Fra",
  "director": "Vicente Escrivá",
  "director_of_photography": "",
  "writers": "Vicente Escrivá",
  "producers": "",
  "music_composer": "",
  "genres": "Comedy",
  "production_companies": "Aspa",
  "production_countries": "Spain",
  "spoken_languages": "Español",
  "poster_path": "/A1bL8cYn1mhKlE2TcovpPj5HqP4.jpg",
  "__op": "r"
}] 
```

### Following is the sample message
```
Key (String): 431150
Value (JSON): {
"id": "431150",
"imdb_id": "tt0068436",
"popularity": 1.686,
"vote_average": 2.0,
"vote_count": 1,
"imdb_rating": 4.8,
"imdb_votes": 20,
"title": "La curiosa",
"original_title": "La curiosa",
"tagline": "",
"overview": "",
"budget": 0,
"revenue": 0,
"runtime": 0,
"status": "Released",
"release_date": "1973-01-24",
"original_language": "es",
"cast": "Roberto Daniel, José Yepes, Rafaela Aparicio, Katy Vadillo, Isabel Pallarés, Ingrid Rabel, Verónica Llimerá, Vicente Roca, José Riesgo, Francisco Ortuño, Mirta Miller, Gustavo Casado, Guadalupe Muñoz Sampedro, Paloma Juanes, Valentina Gutiérrez, Ángel Picazo, Nené Morales, José Fernández, Rosita Fuster, Paco Lara, Cristino Almodóvar, Francisco Agudín, Fabián Conde, Rosa Fontana, Alfonso del Real, Antonio Cintado, E.T. Ruiz, Asunción Aranda, Esther Santana, Pilar Gómez Ferrer, María Isbert, Yolanda Ríos, Manuel de Blas, Carmen Martínez Sierra, Paquita Ruiz, Simón Arriaga, Liliane Meric, Luis Coromina, Máximo Valverde, Luis Barbero, Jimmy Arnau, Betsabé Ruiz, Mari Carmen Prendes, Mari Carmen Duque, Paca Gabaldón, Mery Leyva, Pepita Jiménez, Josele Román, Patty Shepard, Lola Tejela, Beni Deus, Pedro Valentín, José María Fra",
"director": "Vicente Escrivá",
"director_of_photography": "",
"writers": "Vicente Escrivá",
"producers": "",
"music_composer": "",
"genres": "Comedy",
"production_companies": "Aspa",
"production_countries": "Spain",
"spoken_languages": "Español",
"poster_path": "/A1bL8cYn1mhKlE2TcovpPj5HqP4.jpg"
}
```
