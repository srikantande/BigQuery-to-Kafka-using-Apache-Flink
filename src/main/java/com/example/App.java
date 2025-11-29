package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * 12-Factor App compliant Flink application for BigQuery to Kafka streaming.
 * All configurations are externalized following cloud-native best practices.
 */
public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        LOG.info("Starting BigQuery to Kafka Flink Pipeline with externalized configuration");

        // Factor III: Config - Load configuration from external files
        String configPath = System.getenv("CONFIG_PATH");
        if (configPath == null || configPath.isEmpty()) {
            configPath = args.length > 0 ? args[0] : "./config.properties";
        }
        LOG.info("Loading configuration from: {}", configPath);

        Properties config = loadConfiguration(configPath);
        
        // Load schema definition
        String schemaPath = config.getProperty("schema.definition.path");
        LOG.info("Loading schema definition from: {}", schemaPath);
        SchemaDefinition schema = loadSchemaDefinition(schemaPath);

        // Setup Flink Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Determine execution mode from config
        String executionMode = config.getProperty("flink.execution.mode", "batch");
        final TableEnvironment tEnv;
        
        if ("streaming".equalsIgnoreCase(executionMode)) {
            tEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build()
            );
            LOG.info("Flink configured in STREAMING mode");
        } else {
            tEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build()
            );
            LOG.info("Flink configured in BATCH mode");
        }

        // Create BigQuery Source Table with dynamic schema
        String bigQuerySourceDDL = generateBigQuerySourceDDL(config, schema);
        LOG.debug("BigQuery Source DDL:\n{}", bigQuerySourceDDL);
        tEnv.executeSql(bigQuerySourceDDL);
        LOG.info("BigQuery Source Table created: {}", schema.getSourceTableName());

        // Create Kafka Sink Table with dynamic schema
        String kafkaSinkDDL = generateKafkaSinkDDL(config, schema);
        LOG.debug("Kafka Sink DDL:\n{}", kafkaSinkDDL);
        tEnv.executeSql(kafkaSinkDDL);
        LOG.info("Kafka Sink Table created: {}", schema.getSinkTableName());

        // Generate and execute data transformation SQL
        String insertSQL = generateInsertSQL(schema);
        LOG.debug("Insert SQL:\n{}", insertSQL);
        
        try {
            tEnv.executeSql(insertSQL);
            LOG.info("Flink pipeline submitted successfully: {} -> Kafka", 
                     schema.getBigQueryTable());
        } catch (Exception e) {
            LOG.error("Error executing pipeline", e);
            throw e;
        }
    }

    /**
     * Load configuration from external properties file.
     * Supports environment variable overrides following 12-factor principles.
     */
    private static Properties loadConfiguration(String configPath) throws IOException {
        Properties props = new Properties();
        
        File configFile = new File(configPath);
        if (!configFile.exists()) {
            throw new IOException("Configuration file not found: " + configPath);
        }
        
        try (FileInputStream fis = new FileInputStream(configFile)) {
            props.load(fis);
        }
        
        // Factor III: Allow environment variables to override properties
        props.forEach((key, value) -> {
            String envKey = key.toString().toUpperCase().replace('.', '_');
            String envValue = System.getenv(envKey);
            if (envValue != null && !envValue.isEmpty()) {
                props.setProperty(key.toString(), envValue);
                LOG.info("Property '{}' overridden by environment variable", key);
            }
        });
        
        return props;
    }

    /**
     * Load schema definition from external JSON file for any BigQuery table.
     */
    private static SchemaDefinition loadSchemaDefinition(String schemaPath) throws IOException {
        String jsonContent = new String(Files.readAllBytes(Paths.get(schemaPath)));
        JsonNode root = MAPPER.readTree(jsonContent);
        
        SchemaDefinition schema = new SchemaDefinition();
        schema.setSourceTableName(root.get("sourceTableName").asText());
        schema.setSinkTableName(root.get("sinkTableName").asText());
        schema.setBigQueryProject(root.get("bigQueryProject").asText());
        schema.setBigQueryDataset(root.get("bigQueryDataset").asText());
        schema.setBigQueryTable(root.get("bigQueryTable").asText());
        schema.setKafkaTopic(root.get("kafkaTopic").asText());
        
        // Load column definitions
        List<ColumnDefinition> columns = new ArrayList<>();
        JsonNode columnsNode = root.get("columns");
        for (JsonNode colNode : columnsNode) {
            ColumnDefinition col = new ColumnDefinition();
            col.setName(colNode.get("name").asText());
            col.setSourceType(colNode.get("sourceType").asText());
            col.setSinkType(colNode.get("sinkType").asText());
            col.setNullable(colNode.get("nullable").asBoolean());
            col.setKeyField(colNode.has("keyField") && colNode.get("keyField").asBoolean());
            col.setTransform(colNode.has("transform") ? colNode.get("transform").asText() : null);
            columns.add(col);
        }
        schema.setColumns(columns);
        
        LOG.info("Loaded schema with {} columns", columns.size());
        return schema;
    }

    /**
     * Generate BigQuery source DDL dynamically based on schema definition.
     */
    private static String generateBigQuerySourceDDL(Properties config, SchemaDefinition schema) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(schema.getSourceTableName()).append(" (\n");
        
        // Add columns
        for (int i = 0; i < schema.getColumns().size(); i++) {
            ColumnDefinition col = schema.getColumns().get(i);
            ddl.append("  ");
            
            // Handle reserved keywords
            if (isReservedKeyword(col.getName())) {
                ddl.append("`").append(col.getName()).append("`");
            } else {
                ddl.append(col.getName());
            }
            
            ddl.append(" ").append(col.getSourceType());
            
            if (i < schema.getColumns().size() - 1) {
                ddl.append(",\n");
            } else {
                ddl.append("\n");
            }
        }
        
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'bigquery',\n");
        ddl.append("  'project' = '").append(schema.getBigQueryProject()).append("',\n");
        ddl.append("  'dataset' = '").append(schema.getBigQueryDataset()).append("',\n");
        ddl.append("  'table' = '").append(schema.getBigQueryTable()).append("',\n");
        ddl.append("  'credentials.file' = '").append(config.getProperty("bigquery.credentials.path")).append("'\n");
        ddl.append(")");
        
        return ddl.toString();
    }

    /**
     * Generate Kafka sink DDL dynamically based on schema definition.
     */
    private static String generateKafkaSinkDDL(Properties config, SchemaDefinition schema) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(schema.getSinkTableName()).append(" (\n");
        
        List<String> keyFields = new ArrayList<>();
        
        // Add columns
        for (int i = 0; i < schema.getColumns().size(); i++) {
            ColumnDefinition col = schema.getColumns().get(i);
            ddl.append("  ");
            
            // Handle reserved keywords
            if (isReservedKeyword(col.getName())) {
                ddl.append("`").append(col.getName()).append("`");
            } else {
                ddl.append(col.getName());
            }
            
            ddl.append(" ").append(col.getSinkType());
            
            if (col.isKeyField()) {
                keyFields.add(isReservedKeyword(col.getName()) ? 
                             "`" + col.getName() + "`" : col.getName());
            }
            
            if (i < schema.getColumns().size() - 1) {
                ddl.append(",\n");
            } else {
                ddl.append("\n");
            }
        }
        
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'kafka',\n");
        ddl.append("  'topic' = '").append(schema.getKafkaTopic()).append("',\n");
        ddl.append("  'properties.bootstrap.servers' = '").append(config.getProperty("kafka.bootstrap.servers")).append("',\n");
        ddl.append("  'key.format' = 'raw',\n");
        
        if (!keyFields.isEmpty()) {
            ddl.append("  'key.fields' = '").append(String.join(";", keyFields)).append("',\n");
        }
        
        ddl.append("  'value.format' = '").append(config.getProperty("kafka.value.format", "json")).append("'\n");
        ddl.append(")");
        
        return ddl.toString();
    }

    /**
     * Generate INSERT SQL with transformations from schema definition.
     */
    private static String generateInsertSQL(SchemaDefinition schema) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(schema.getSinkTableName()).append("\n");
        sql.append("SELECT\n");
        
        for (int i = 0; i < schema.getColumns().size(); i++) {
            ColumnDefinition col = schema.getColumns().get(i);
            sql.append("  ");
            
            String columnRef = isReservedKeyword(col.getName()) ? 
                              "`" + col.getName() + "`" : col.getName();
            
            if (col.getTransform() != null && !col.getTransform().isEmpty()) {
                // Apply custom transformation
                sql.append(col.getTransform().replace("${column}", columnRef));
            } else if (col.isNullable() && !col.getSourceType().equals(col.getSinkType())) {
                // Default transformation: NULLIF for nullable fields with type conversion
                sql.append("CAST(NULLIF(").append(columnRef).append(", '') AS ")
                   .append(col.getSinkType()).append(")");
            } else if (!col.getSourceType().equals(col.getSinkType())) {
                // Simple cast
                sql.append("CAST(").append(columnRef).append(" AS ")
                   .append(col.getSinkType()).append(")");
            } else {
                // No transformation needed
                sql.append(columnRef);
            }
            
            if (i < schema.getColumns().size() - 1) {
                sql.append(",\n");
            } else {
                sql.append("\n");
            }
        }
        
        sql.append("FROM ").append(schema.getSourceTableName());
        return sql.toString();
    }

    /**
     * Check if column name is a SQL reserved keyword.
     */
    private static boolean isReservedKeyword(String name) {
        Set<String> keywords = new HashSet<>(Arrays.asList(
            "cast", "key", "value", "order", "group", "select", "from", "where", "table"
        ));
        return keywords.contains(name.toLowerCase());
    }

    // Inner classes for schema representation
    static class SchemaDefinition {
        private String sourceTableName;
        private String sinkTableName;
        private String bigQueryProject;
        private String bigQueryDataset;
        private String bigQueryTable;
        private String kafkaTopic;
        private List<ColumnDefinition> columns;

        // Getters and setters
        public String getSourceTableName() { return sourceTableName; }
        public void setSourceTableName(String sourceTableName) { this.sourceTableName = sourceTableName; }
        public String getSinkTableName() { return sinkTableName; }
        public void setSinkTableName(String sinkTableName) { this.sinkTableName = sinkTableName; }
        public String getBigQueryProject() { return bigQueryProject; }
        public void setBigQueryProject(String bigQueryProject) { this.bigQueryProject = bigQueryProject; }
        public String getBigQueryDataset() { return bigQueryDataset; }
        public void setBigQueryDataset(String bigQueryDataset) { this.bigQueryDataset = bigQueryDataset; }
        public String getBigQueryTable() { return bigQueryTable; }
        public void setBigQueryTable(String bigQueryTable) { this.bigQueryTable = bigQueryTable; }
        public String getKafkaTopic() { return kafkaTopic; }
        public void setKafkaTopic(String kafkaTopic) { this.kafkaTopic = kafkaTopic; }
        public List<ColumnDefinition> getColumns() { return columns; }
        public void setColumns(List<ColumnDefinition> columns) { this.columns = columns; }
    }

    static class ColumnDefinition {
        private String name;
        private String sourceType;
        private String sinkType;
        private boolean nullable;
        private boolean keyField;
        private String transform;

        // Getters and setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getSourceType() { return sourceType; }
        public void setSourceType(String sourceType) { this.sourceType = sourceType; }
        public String getSinkType() { return sinkType; }
        public void setSinkType(String sinkType) { this.sinkType = sinkType; }
        public boolean isNullable() { return nullable; }
        public void setNullable(boolean nullable) { this.nullable = nullable; }
        public boolean isKeyField() { return keyField; }
        public void setKeyField(boolean keyField) { this.keyField = keyField; }
        public String getTransform() { return transform; }
        public void setTransform(String transform) { this.transform = transform; }
    }
}
