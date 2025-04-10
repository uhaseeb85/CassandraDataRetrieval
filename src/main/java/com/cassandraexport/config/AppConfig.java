package com.cassandraexport.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
    private static final String CONFIG_FILE = "application.properties";
    private static AppConfig instance;
    private final Properties properties;

    private AppConfig() {
        properties = new Properties();
        loadProperties();
    }

    public static synchronized AppConfig getInstance() {
        if (instance == null) {
            instance = new AppConfig();
        }
        return instance;
    }

    private void loadProperties() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                logger.error("Unable to find {}", CONFIG_FILE);
                throw new RuntimeException("Unable to find " + CONFIG_FILE);
            }
            properties.load(input);
            logger.info("Loaded configuration from {}", CONFIG_FILE);
        } catch (IOException ex) {
            logger.error("Error loading properties file: {}", ex.getMessage(), ex);
            throw new RuntimeException("Error loading properties file", ex);
        }
    }

    public String getCassandraContactPoints() {
        return properties.getProperty("cassandra.contactPoints");
    }

    public int getCassandraPort() {
        return Integer.parseInt(properties.getProperty("cassandra.port", "9042"));
    }

    public String getCassandraLocalDatacenter() {
        return properties.getProperty("cassandra.localDatacenter");
    }

    public String getCassandraKeyspace() {
        return properties.getProperty("cassandra.keyspace");
    }

    public String getCassandraTable() {
        return properties.getProperty("cassandra.table");
    }

    public String getCassandraUsername() {
        return properties.getProperty("cassandra.username");
    }

    public String getCassandraPassword() {
        return properties.getProperty("cassandra.password");
    }

    public String getCassandraQuery() {
        return properties.getProperty("cassandra.query");
    }

    public int getCassandraBatchSize() {
        return Integer.parseInt(properties.getProperty("cassandra.batchSize", "10000"));
    }

    public long getCassandraTotalRecords() {
        return Long.parseLong(properties.getProperty("cassandra.totalRecords", "6000000"));
    }

    public String getStateCheckpointFile() {
        return properties.getProperty("state.checkpointFile", "checkpoint.json");
    }

    public String getKafkaBootstrapServers() {
        return properties.getProperty("kafka.bootstrapServers");
    }

    public String getKafkaTopic() {
        return properties.getProperty("kafka.topic");
    }

    public String getKafkaClientId() {
        return properties.getProperty("kafka.clientId");
    }

    public String getKafkaAcks() {
        return properties.getProperty("kafka.acks", "all");
    }

    public int getKafkaRetries() {
        return Integer.parseInt(properties.getProperty("kafka.retries", "10"));
    }

    public int getKafkaBatchSize() {
        return Integer.parseInt(properties.getProperty("kafka.batchSize", "16384"));
    }

    public int getKafkaBufferMemory() {
        return Integer.parseInt(properties.getProperty("kafka.bufferMemory", "33554432"));
    }

    public int getErrorMaxRetries() {
        return Integer.parseInt(properties.getProperty("error.maxRetries", "5"));
    }

    public int getErrorRetryBackoffMs() {
        return Integer.parseInt(properties.getProperty("error.retryBackoffMs", "1000"));
    }
} 