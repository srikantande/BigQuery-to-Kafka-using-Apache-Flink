# BigQuery-to-Kafka-using-Apache-Flink
BigQuery to Kafka using Apache Flink

**What it does:** Reads your config → Connects BigQuery → Connects Kafka → Moves data automatically.

**No coding needed!** Edit `config.properties` + `schema.json` for any BigQuery table.

## 🔍 File Explanations (No Coding Knowledge Needed)

### `pom.xml` - 📦 The Build Recipe

<!-- 🎯 THIS IS YOUR PROJECT'S IDENTITY CARD -->
<groupId>com.example</groupId>          <!-- 👨‍💼 Your company/team name -->
<artifactId>bigquery-to-kafka-flink</artifactId>  <!-- 📱 App name -->
<version>1.0-SNAPSHOT</version>         <!-- 🏷️ Version (SNAPSHOT = "work in progress") -->

<!-- ⚙️ SETTINGS: Like choosing "Easy Mode" -->
<properties>
  <maven.compiler.source>11</maven.compiler.source>  <!-- ☕ Use Java 11 -->
  <flink.version>1.18.0</flink.version>              <!-- 🔗 Flink version -->
</properties>

<!-- 🧩 PARTS LIST: Libraries this app needs -->
<dependencies>
  <!-- Flink "engine" for data processing -->
  <dependency><groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId></dependency>
  
  <!-- Flink "SQL translator" -->
  <dependency><groupId>org.apache.flink</groupId><artifactId>flink-table-api-java-bridge</artifactId></dependency>
  
  <!-- 🔌 Kafka plugin -->
  <dependency><groupId>org.apache.flink</groupId><artifactId>flink-connector-kafka</artifactId><version>3.1.0-1.18</version></dependency>
  
  <!-- 🔌 BigQuery plugin -->
  <dependency><groupId>com.google.cloud.flink</groupId><artifactId>flink-1.17-connector-bigquery</artifactId></dependency>
</dependencies>

<!-- 🚫 "DO NOT PACK THESE" - Keeps config files OUTSIDE the app -->
<resources>
  <excludes>
    <exclude>**/*.properties</exclude>  <!-- config.properties stays external -->
    <exclude>**/*.json</exclude>        <!-- schema.json stays external -->
  </excludes>
</resources>

<!-- 🔨 BUILD INSTRUCTIONS: Makes "fat JAR" with mainClass entry -->
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>  <!-- 📦 Bundles everything into 1 file -->
  <mainClass>com.example.App</mainClass>       <!-- 🎮 Where to start the app -->
</plugin>

**What it does:** Downloads Flink + Kafka + BigQuery libraries, compiles Java, creates executable JAR.

### `App.java` - ⚡ The Data Pipeline Brain

public class App {  // 🏠 Main application class
    public static void main(String[] args) throws Exception {  // 🚀 Entry point
        
        // 📁 STEP 1: Read config files (external, not inside JAR)
        String configPath = args.length > 0 ? args[0] : "./config.properties";
        Properties config = loadConfiguration(configPath);  // 📖 Load settings
        
        // 📊 STEP 2: Read schema.json (defines your BigQuery table)
        SchemaDefinition schema = loadSchemaDefinition(config.getProperty("schema.definition.path"));
        
        // 🛠️ STEP 3: Setup Flink (batch or streaming mode)
        TableEnvironment tEnv = /* batch or streaming based on config */;
        
        // 🔧 STEP 4: Auto-generate SQL (magic happens here!)
        String bigQueryDDL = generateBigQuerySourceDDL(config, schema);  // Creates BigQuery table
        tEnv.executeSql(bigQuerySourceDDL);  // 📥 "Connect to BigQuery"
        
        String kafkaDDL = generateKafkaSinkDDL(config, schema);         // Creates Kafka table
        tEnv.executeSql(kafkaDDL);  // 📤 "Connect to Kafka"
        
        // ▶️ STEP 5: Run pipeline! (BigQuery → Kafka)
        String insertSQL = generateInsertSQL(schema);  // Auto-transforms data types
        tEnv.executeSql(insertSQL);  // 🎉 Data flows!
    }
    
    // 🛠️ Helper: Reads config.properties with ENV override support
    private static Properties loadConfiguration(String configPath) { /* ... */ }
    
    // 📋 Helper: Parses schema.json (your table definition)
    private static SchemaDefinition loadSchemaDefinition(String schemaPath) { /* ... */ }
    
    // ✨ Magic: Creates BigQuery table SQL from JSON schema
    private static String generateBigQuerySourceDDL(Properties config, SchemaDefinition schema) { /* ... */ }
    
    // ✨ Magic: Creates Kafka table SQL from JSON schema  
    private static String generateKafkaSinkDDL(Properties config, SchemaDefinition schema) { /* ... */ }
    
    // 🔄 Magic: Creates INSERT with type casting & null safety
    private static String generateInsertSQL(SchemaDefinition schema) { /* ... */ }
}

