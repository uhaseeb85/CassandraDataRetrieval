package com.cassandraexport.kafka;

import com.cassandraexport.config.AppConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final AppConfig config;
    private Producer<String, String> producer;
    private final AtomicInteger retryCounter = new AtomicInteger(0);
    private final int maxRetries;
    private final long retryBackoffMs;
    
    public KafkaProducer() {
        this.config = AppConfig.getInstance();
        this.maxRetries = config.getErrorMaxRetries();
        this.retryBackoffMs = config.getErrorRetryBackoffMs();
        initializeProducer();
    }
    
    private void initializeProducer() {
        logger.info("Initializing Kafka producer to {}", config.getKafkaBootstrapServers());
        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
            props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getKafkaClientId());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, config.getKafkaAcks());
            props.put(ProducerConfig.RETRIES_CONFIG, config.getKafkaRetries());
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getKafkaBatchSize());
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getKafkaBufferMemory());
            
            // Create the Kafka producer
            producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
            logger.info("Kafka producer initialized successfully");
        } catch (KafkaException e) {
            logger.error("Failed to initialize Kafka producer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize Kafka producer", e);
        }
    }

    public boolean sendRecord(String key, Map<String, Object> record) throws InterruptedException {
        try {
            String recordJson = objectMapper.writeValueAsString(record);
            String topic = config.getKafkaTopic();
            
            ProducerRecord<String, String> producerRecord = 
                    new ProducerRecord<>(topic, key, recordJson);
            
            // Send the record with retry logic
            return sendWithRetry(producerRecord);
            
        } catch (Exception e) {
            logger.error("Error serializing record to JSON: {}", e.getMessage(), e);
            return false;
        }
    }
    
    private boolean sendWithRetry(ProducerRecord<String, String> record) throws InterruptedException {
        int attempts = 0;
        boolean sent = false;
        Exception lastException = null;
        
        while (!sent && attempts < maxRetries) {
            try {
                Future<RecordMetadata> future = producer.send(record);
                
                // Wait for confirmation (with timeout)
                RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
                
                logger.debug("Record sent to partition {} with offset {}", 
                        metadata.partition(), metadata.offset());
                
                sent = true;
                retryCounter.set(0); // Reset retry counter on success
                
            } catch (ExecutionException | TimeoutException e) {
                attempts++;
                lastException = e;
                retryCounter.incrementAndGet();
                
                long waitTime = Math.min(retryBackoffMs * attempts, 10000); // Max 10 seconds
                logger.warn("Failed to send record to Kafka (attempt {}/{}), retrying in {} ms: {}", 
                        attempts, maxRetries, waitTime, e.getMessage());
                
                Thread.sleep(waitTime);
            }
        }
        
        if (!sent) {
            logger.error("Failed to send record to Kafka after {} attempts: {}", 
                    attempts, lastException.getMessage(), lastException);
        }
        
        return sent;
    }
    
    public void flush() {
        if (producer != null) {
            try {
                producer.flush();
                logger.debug("Flushed Kafka producer");
            } catch (KafkaException e) {
                logger.error("Error flushing Kafka producer: {}", e.getMessage(), e);
            }
        }
    }
    
    public int getFailedAttempts() {
        return retryCounter.get();
    }
    
    public boolean isHealthy() {
        return retryCounter.get() < maxRetries;
    }
    
    @Override
    public void close() {
        if (producer != null) {
            try {
                producer.flush();
                producer.close();
                logger.info("Kafka producer closed");
            } catch (KafkaException e) {
                logger.error("Error closing Kafka producer: {}", e.getMessage(), e);
            }
        }
    }
} 