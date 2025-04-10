package com.cassandraexport.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class AppConfigTest {

    @BeforeEach
    void setUp() {
        // Reset AppConfig singleton before each test
        TestAppConfig.resetInstance();
    }
    
    @AfterEach
    void tearDown() {
        // Reset AppConfig singleton after each test
        TestAppConfig.resetInstance();
    }
    
    @Test
    void testLoadConfigFromFile() {
        // This will load application.properties from test/resources
        AppConfig config = AppConfig.getInstance();
        
        // Verify Cassandra config
        assertEquals("localhost", config.getCassandraContactPoints());
        assertEquals(9042, config.getCassandraPort());
        assertEquals("datacenter1", config.getCassandraLocalDatacenter());
        assertEquals("testks", config.getCassandraKeyspace());
        assertEquals("testtable", config.getCassandraTable());
        assertEquals("SELECT * FROM testks.testtable", config.getCassandraQuery());
        assertEquals(100, config.getCassandraBatchSize());
        assertEquals(1000, config.getCassandraTotalRecords());
        
        // Verify Kafka config
        assertEquals("localhost:9092", config.getKafkaBootstrapServers());
        assertEquals("test-topic", config.getKafkaTopic());
        assertEquals("test-client", config.getKafkaClientId());
        assertEquals("all", config.getKafkaAcks());
        assertEquals(3, config.getKafkaRetries());
        assertEquals(16384, config.getKafkaBatchSize());
        assertEquals(33554432, config.getKafkaBufferMemory());
        
        // Verify error handling config
        assertEquals(3, config.getErrorMaxRetries());
        assertEquals(100, config.getErrorRetryBackoffMs());
        
        // Verify checkpoint file - using trim() to handle any potential whitespace
        assertEquals("test-checkpoint.json", config.getStateCheckpointFile().trim());
    }
    
    @Test
    void testInjectCustomProperties() {
        // Create custom properties
        Properties customProps = new Properties();
        customProps.setProperty("cassandra.contactPoints", "customhost");
        customProps.setProperty("cassandra.port", "9999");
        customProps.setProperty("kafka.topic", "custom-topic");
        
        // Inject custom properties
        TestAppConfig.injectTestProperties(customProps);
        AppConfig config = AppConfig.getInstance();
        
        // Verify custom properties were applied
        assertEquals("customhost", config.getCassandraContactPoints());
        assertEquals(9999, config.getCassandraPort());
        assertEquals("custom-topic", config.getKafkaTopic());
        
        // Other properties should have default values
        assertEquals("all", config.getKafkaAcks());
        assertEquals(10, config.getKafkaRetries());
    }
} 