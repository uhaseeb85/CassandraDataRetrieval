package com.cassandraexport;

import com.cassandraexport.cassandra.CassandraClient;
import com.cassandraexport.config.TestAppConfig;
import com.cassandraexport.kafka.KafkaProducer;
import com.cassandraexport.model.CheckpointState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CassandraToKafkaExporterTest {

    @Mock
    private CheckpointState mockCheckpointState;
    
    @TempDir
    Path tempDir;
    
    private Properties testProperties;
    private String checkpointFile;
    
    @BeforeEach
    void setUp() {
        // Set up test properties
        testProperties = new Properties();
        testProperties.setProperty("cassandra.batchSize", "100");
        testProperties.setProperty("cassandra.totalRecords", "1000"); // Set to 1000 for test completion verification
        
        // Configure checkpoint file in temp directory
        checkpointFile = tempDir.resolve("test-checkpoint.json").toString();
        testProperties.setProperty("state.checkpointFile", checkpointFile);
        
        // Inject test properties
        TestAppConfig.injectTestProperties(testProperties);
    }
    
    @Test
    void testStartNewExport() throws Exception {
        // Use mocked static for CheckpointState.load
        try (MockedStatic<CheckpointState> mockedCheckpointState = Mockito.mockStatic(CheckpointState.class)) {
            // Mock CheckpointState.load() to return our mock
            mockedCheckpointState.when(() -> CheckpointState.load(anyString())).thenReturn(mockCheckpointState);
            when(mockCheckpointState.isCompleted()).thenReturn(false);
            when(mockCheckpointState.getLastProcessedOffset()).thenReturn(0L);
            when(mockCheckpointState.getRecordsProcessed()).thenReturn(0L);
            
            // Mock sample data
            List<Map<String, Object>> sampleBatch1 = createSampleBatch(5, 0);
            List<Map<String, Object>> sampleBatch2 = createSampleBatch(5, 5);
            List<Map<String, Object>> emptyBatch = new ArrayList<>();
            
            // Mock CassandraClient and KafkaProducer
            try (MockedConstruction<CassandraClient> mockedCassandraClient = Mockito.mockConstruction(CassandraClient.class,
                    (mock, context) -> {
                        // Return sample batches in sequence
                        when(mock.fetchBatch(0, 100)).thenReturn(sampleBatch1);
                        when(mock.fetchBatch(5, 100)).thenReturn(sampleBatch2);
                        when(mock.fetchBatch(10, 100)).thenReturn(emptyBatch);
                    });
                 MockedConstruction<KafkaProducer> mockedKafkaProducer = Mockito.mockConstruction(KafkaProducer.class,
                    (mock, context) -> {
                        when(mock.sendRecord(anyString(), any(Map.class))).thenReturn(true);
                        when(mock.isHealthy()).thenReturn(true);
                    })
            ) {
                // Create and run the exporter
                CassandraToKafkaExporter exporter = new CassandraToKafkaExporter();
                exporter.start();
                
                // Verify CassandraClient and KafkaProducer were constructed
                assertEquals(1, mockedCassandraClient.constructed().size());
                assertEquals(1, mockedKafkaProducer.constructed().size());
                
                // Verify batch processing
                verify(mockCheckpointState, times(2)).updateProgress(anyLong(), anyInt());
                
                // Verify markCompleted is not called since we only processed 10 records
                // and the totalRecords in our config is 1000
                verify(mockCheckpointState, never()).markCompleted();
            }
        }
    }
    
    @Test
    void testResumeExport() throws Exception {
        // Use mocked static for CheckpointState.load
        try (MockedStatic<CheckpointState> mockedCheckpointState = Mockito.mockStatic(CheckpointState.class)) {
            // Mock CheckpointState.load() to return our mock - simulating a previously interrupted job
            mockedCheckpointState.when(() -> CheckpointState.load(anyString())).thenReturn(mockCheckpointState);
            when(mockCheckpointState.isCompleted()).thenReturn(false);
            when(mockCheckpointState.getLastProcessedOffset()).thenReturn(5L); // Resume from offset 5
            when(mockCheckpointState.getRecordsProcessed()).thenReturn(5L);
            when(mockCheckpointState.getErrorMessage()).thenReturn("Previous error");
            
            // Mock sample data
            List<Map<String, Object>> sampleBatch2 = createSampleBatch(5, 5);
            List<Map<String, Object>> emptyBatch = new ArrayList<>();
            
            // Mock CassandraClient and KafkaProducer
            try (MockedConstruction<CassandraClient> mockedCassandraClient = Mockito.mockConstruction(CassandraClient.class,
                    (mock, context) -> {
                        // Return sample batches in sequence - starting from resume point
                        when(mock.fetchBatch(5, 100)).thenReturn(sampleBatch2);
                        when(mock.fetchBatch(10, 100)).thenReturn(emptyBatch);
                    });
                 MockedConstruction<KafkaProducer> mockedKafkaProducer = Mockito.mockConstruction(KafkaProducer.class,
                    (mock, context) -> {
                        when(mock.sendRecord(anyString(), any(Map.class))).thenReturn(true);
                        when(mock.isHealthy()).thenReturn(true);
                    })
            ) {
                // Create and run the exporter
                CassandraToKafkaExporter exporter = new CassandraToKafkaExporter();
                exporter.start();
                
                // Verify resumed from correct point
                CassandraClient cassandraClient = mockedCassandraClient.constructed().get(0);
                verify(cassandraClient).fetchBatch(5, 100);
                
                // Verify only one batch was processed from the resume point
                verify(mockCheckpointState, times(1)).updateProgress(anyLong(), anyInt());
                
                // Verify markCompleted is not called since we only processed 10 records total
                // and the totalRecords in our config is 1000
                verify(mockCheckpointState, never()).markCompleted();
            }
        }
    }
    
    @Test
    void testAlreadyCompletedExport() throws Exception {
        // Use mocked static for CheckpointState.load
        try (MockedStatic<CheckpointState> mockedCheckpointState = Mockito.mockStatic(CheckpointState.class)) {
            // Mock CheckpointState.load() to return our mock - simulating a completed job
            mockedCheckpointState.when(() -> CheckpointState.load(anyString())).thenReturn(mockCheckpointState);
            when(mockCheckpointState.isCompleted()).thenReturn(true);
            
            // Create and run the exporter
            CassandraToKafkaExporter exporter = new CassandraToKafkaExporter();
            exporter.start();
            
            // Verify we don't try to process anything if job is already marked complete
            verify(mockCheckpointState, never()).updateProgress(anyLong(), anyInt());
            verify(mockCheckpointState, never()).markCompleted();
        }
    }
    
    @Test
    void testBatchFailure() throws Exception {
        // Use mocked static for CheckpointState.load
        try (MockedStatic<CheckpointState> mockedCheckpointState = Mockito.mockStatic(CheckpointState.class)) {
            // Mock CheckpointState.load() to return our mock
            mockedCheckpointState.when(() -> CheckpointState.load(anyString())).thenReturn(mockCheckpointState);
            when(mockCheckpointState.isCompleted()).thenReturn(false);
            when(mockCheckpointState.getLastProcessedOffset()).thenReturn(0L);
            when(mockCheckpointState.getRecordsProcessed()).thenReturn(0L);
            
            // Mock sample data
            List<Map<String, Object>> sampleBatch = createSampleBatch(5, 0);
            
            // Mock CassandraClient and KafkaProducer with failure
            try (MockedConstruction<CassandraClient> mockedCassandraClient = Mockito.mockConstruction(CassandraClient.class,
                    (mock, context) -> {
                        when(mock.fetchBatch(0, 100)).thenReturn(sampleBatch);
                    });
                 MockedConstruction<KafkaProducer> mockedKafkaProducer = Mockito.mockConstruction(KafkaProducer.class,
                    (mock, context) -> {
                        // Simulate failures for all records
                        when(mock.sendRecord(anyString(), any(Map.class))).thenReturn(false);
                    })
            ) {
                // Create and run the exporter
                CassandraToKafkaExporter exporter = new CassandraToKafkaExporter();
                exporter.start();
                
                // Verify error was set and no progress was made
                verify(mockCheckpointState, never()).updateProgress(anyLong(), anyInt());
                verify(mockCheckpointState, times(1)).setError(anyString());
            }
        }
    }
    
    @Test
    void testCompleteExport() throws Exception {
        // Use mocked static for CheckpointState.load
        try (MockedStatic<CheckpointState> mockedCheckpointState = Mockito.mockStatic(CheckpointState.class)) {
            // Mock CheckpointState.load() to return our mock
            mockedCheckpointState.when(() -> CheckpointState.load(anyString())).thenReturn(mockCheckpointState);
            when(mockCheckpointState.isCompleted()).thenReturn(false);
            when(mockCheckpointState.getLastProcessedOffset()).thenReturn(0L);
            when(mockCheckpointState.getRecordsProcessed()).thenReturn(0L);
            
            // Override test properties to set a smaller totalRecords value
            Properties smallConfig = new Properties();
            smallConfig.putAll(testProperties); // Keep existing properties
            smallConfig.setProperty("cassandra.totalRecords", "5");
            smallConfig.setProperty("cassandra.batchSize", "5");
            TestAppConfig.injectTestProperties(smallConfig);
            
            // Mock sample data - just enough to complete
            List<Map<String, Object>> sampleBatch = createSampleBatch(5, 0);
            List<Map<String, Object>> emptyBatch = new ArrayList<>();
            
            // Mock CassandraClient and KafkaProducer
            try (MockedConstruction<CassandraClient> mockedCassandraClient = Mockito.mockConstruction(CassandraClient.class,
                    (mock, context) -> {
                        // Mock fetchBatch to return data
                        when(mock.fetchBatch(0, 5)).thenReturn(sampleBatch);
                        when(mock.fetchBatch(5, 5)).thenReturn(emptyBatch);
                        when(mock.isConnected()).thenReturn(true);
                    });
                 MockedConstruction<KafkaProducer> mockedKafkaProducer = Mockito.mockConstruction(KafkaProducer.class,
                    (mock, context) -> {
                        // Mock sendRecord to always succeed
                        when(mock.sendRecord(anyString(), any(Map.class))).thenReturn(true);
                        when(mock.isHealthy()).thenReturn(true);
                    })
            ) {
                // Create and run the exporter
                CassandraToKafkaExporter exporter = new CassandraToKafkaExporter();
                exporter.start();
                
                // Verify progress was updated for the batch
                verify(mockCheckpointState, times(1)).updateProgress(eq(5L), eq(5));
                
                // Verify markCompleted is called since we've processed enough records to complete
                verify(mockCheckpointState, times(1)).markCompleted();
            }
        }
    }
    
    // Helper method to create a sample batch of records
    private List<Map<String, Object>> createSampleBatch(int size, int startOffset) {
        List<Map<String, Object>> batch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Map<String, Object> record = new HashMap<>();
            int recordNum = startOffset + i;
            record.put("id", String.valueOf(recordNum));
            record.put("name", "Test Record " + recordNum);
            record.put("value", recordNum * 10);
            batch.add(record);
        }
        return batch;
    }
}