package com.cassandraexport.kafka;

import com.cassandraexport.config.AppConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaProducerTest {

    private KafkaProducer kafkaProducer;
    private AppConfig config;
    @Mock
    private org.apache.kafka.clients.producer.KafkaProducer<String, String> mockProducer;
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() {
        config = mock(AppConfig.class, withSettings().lenient());
        when(config.getErrorMaxRetries()).thenReturn(3);
        when(config.getErrorRetryBackoffMs()).thenReturn(100);
        when(config.getKafkaTopic()).thenReturn("test-topic");
        
        kafkaProducer = new KafkaProducer(config, true); // Use test constructor that skips initialization
        ReflectionTestUtils.setField(kafkaProducer, "producer", mockProducer);
    }
    
    @Test
    void testSendRecordSuccess() throws Exception {
        // Arrange
        Map<String, Object> record = Map.of("id", "1", "name", "Test");
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        RecordMetadata metadata = new RecordMetadata(topicPartition, 0L, 0L, 0L, Long.valueOf(0L), 0, 0);
        CompletableFuture<RecordMetadata> future = CompletableFuture.completedFuture(metadata);
        
        when(mockProducer.send(any(ProducerRecord.class))).thenReturn(future);
        
        // Act
        boolean result = kafkaProducer.sendRecord("test-key", record);
        
        // Assert
        assertTrue(result);
        verify(mockProducer).send(any(ProducerRecord.class));
    }
    
    @Test
    void testSendRecordFailure() throws Exception {
        // Arrange
        Map<String, Object> record = Map.of("id", "1", "name", "Test");
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Test exception"));
        
        when(mockProducer.send(any(ProducerRecord.class))).thenReturn(future);
        
        // Act
        boolean result = kafkaProducer.sendRecord("test-key", record);
        
        // Assert
        assertFalse(result);
        verify(mockProducer, times(3)).send(any(ProducerRecord.class)); // Verify 3 attempts due to retry logic
    }
    
    @Test
    void testSendRecordTimeout() throws Exception {
        // Arrange
        Map<String, Object> record = Map.of("id", "1", "name", "Test");
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(new TimeoutException("Timed out"));
        
        when(mockProducer.send(any(ProducerRecord.class))).thenReturn(future);
        
        // Act
        boolean result = kafkaProducer.sendRecord("test-key", record);
        
        // Assert
        assertFalse(result);
        verify(mockProducer, times(3)).send(any(ProducerRecord.class)); // Verify 3 attempts due to retry logic
    }
    
    @Test
    void testFlush() {
        // Act
        kafkaProducer.flush();
        
        // Assert
        verify(mockProducer).flush();
    }
    
    @Test
    void testClose() {
        // Act
        kafkaProducer.close();
        
        // Assert
        verify(mockProducer).flush();
        verify(mockProducer).close();
    }
    
    @Test
    void testIsHealthy() {
        // Arrange - by default the producer is healthy
        
        // Act
        boolean result = kafkaProducer.isHealthy();
        
        // Assert
        assertTrue(result);
    }
} 