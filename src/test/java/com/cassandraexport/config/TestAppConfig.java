package com.cassandraexport.config;

import java.util.Properties;
import java.lang.reflect.Field;

/**
 * A helper class for tests that need to mock or override the AppConfig singleton
 */
public class TestAppConfig {
    
    /**
     * Resets the AppConfig singleton instance for testing
     */
    public static void resetInstance() {
        try {
            Field instance = AppConfig.class.getDeclaredField("instance");
            instance.setAccessible(true);
            instance.set(null, null);
        } catch (Exception e) {
            throw new RuntimeException("Could not reset AppConfig instance", e);
        }
    }
    
    /**
     * Sets up a test configuration with default test values
     * @return Properties populated with test values
     */
    public static Properties getTestProperties() {
        Properties props = new Properties();
        
        // Cassandra Configuration
        props.setProperty("cassandra.contactPoints", "localhost");
        props.setProperty("cassandra.port", "9042");
        props.setProperty("cassandra.localDatacenter", "datacenter1");
        props.setProperty("cassandra.keyspace", "testks");
        props.setProperty("cassandra.table", "testtable");
        props.setProperty("cassandra.query", "SELECT * FROM testks.testtable");
        props.setProperty("cassandra.batchSize", "100");
        props.setProperty("cassandra.totalRecords", "1000");
        
        // Kafka Configuration
        props.setProperty("kafka.bootstrapServers", "localhost:9092");
        props.setProperty("kafka.topic", "test-topic");
        props.setProperty("kafka.clientId", "test-client");
        props.setProperty("kafka.acks", "all");
        props.setProperty("kafka.retries", "3");
        props.setProperty("kafka.batchSize", "16384");
        props.setProperty("kafka.bufferMemory", "33554432");
        
        // Error Handling
        props.setProperty("error.maxRetries", "3");
        props.setProperty("error.retryBackoffMs", "100");
        
        // State Checkpoint
        props.setProperty("state.checkpointFile", "test-checkpoint.json");
        
        return props;
    }
    
    /**
     * Injects test properties into the AppConfig singleton
     * @param properties The properties to inject
     */
    public static void injectTestProperties(Properties properties) {
        try {
            resetInstance();
            AppConfig config = AppConfig.getInstance();
            
            Field propsField = AppConfig.class.getDeclaredField("properties");
            propsField.setAccessible(true);
            propsField.set(config, properties);
        } catch (Exception e) {
            throw new RuntimeException("Could not inject test properties", e);
        }
    }
} 