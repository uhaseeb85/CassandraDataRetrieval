package com.cassandraexport;

import com.cassandraexport.cassandra.CassandraClient;
import com.cassandraexport.config.AppConfig;
import com.cassandraexport.kafka.KafkaProducer;
import com.cassandraexport.model.CheckpointState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class CassandraToKafkaExporter {
    private static final Logger logger = LoggerFactory.getLogger(CassandraToKafkaExporter.class);
    private final AppConfig config;
    private final CheckpointState checkpointState;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String checkpointFile;

    public CassandraToKafkaExporter() {
        this.config = AppConfig.getInstance();
        this.checkpointFile = config.getStateCheckpointFile();
        this.checkpointState = CheckpointState.load(checkpointFile);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown initiated, gracefully stopping...");
            running.set(false);
        }));
    }

    public void start() {
        logger.info("Starting Cassandra to Kafka export process");
        logger.info("Batch size: {}, Total target records: {}", 
                config.getCassandraBatchSize(), config.getCassandraTotalRecords());

        if (checkpointState.isCompleted()) {
            logger.info("Previous export job already completed successfully. To run again, delete the checkpoint file.");
            return;
        }

        // If there was an error in a previous run, log it
        if (checkpointState.getErrorMessage() != null) {
            logger.warn("Previous run ended with error: {}. Resuming from last successful offset: {}", 
                    checkpointState.getErrorMessage(), checkpointState.getLastProcessedOffset());
        }

        try (CassandraClient cassandraClient = new CassandraClient();
             KafkaProducer kafkaProducer = new KafkaProducer()) {

            long startOffset = checkpointState.getLastProcessedOffset();
            long totalRecordsToProcess = config.getCassandraTotalRecords();
            int batchSize = config.getCassandraBatchSize();
            long totalProcessed = checkpointState.getRecordsProcessed();

            logger.info("Resuming from offset: {}, already processed: {} records", 
                    startOffset, totalProcessed);

            // Process batches until we've processed all records
            while (running.get() && totalProcessed < totalRecordsToProcess) {
                List<Map<String, Object>> batch = cassandraClient.fetchBatch(startOffset, batchSize);
                
                if (batch.isEmpty()) {
                    logger.info("No more records available from Cassandra");
                    break;
                }

                // Process this batch
                boolean batchSuccess = processBatch(batch, kafkaProducer, startOffset);
                
                if (batchSuccess) {
                    // Update checkpoint
                    int recordsInBatch = batch.size();
                    totalProcessed += recordsInBatch;
                    startOffset += recordsInBatch;
                    
                    checkpointState.updateProgress(startOffset, recordsInBatch);
                    checkpointState.save(checkpointFile);
                    
                    logger.info("Progress: {}/{} records processed ({}%)", 
                            totalProcessed, totalRecordsToProcess, 
                            (totalProcessed * 100) / totalRecordsToProcess);
                    
                } else {
                    // Handle batch failure
                    String errorMsg = "Failed to process batch starting at offset " + startOffset;
                    logger.error(errorMsg);
                    checkpointState.setError(errorMsg);
                    checkpointState.save(checkpointFile);
                    
                    // Force exit the loop
                    break;
                }

                // Check if Kafka producer is still healthy
                if (!kafkaProducer.isHealthy()) {
                    logger.error("Kafka producer is not healthy, stopping export process");
                    String errorMsg = "Kafka producer failed after multiple retries";
                    checkpointState.setError(errorMsg);
                    checkpointState.save(checkpointFile);
                    break;
                }
            }

            // If we completed all records or were shut down gracefully
            if (totalProcessed >= totalRecordsToProcess || !running.get()) {
                if (totalProcessed >= totalRecordsToProcess) {
                    logger.info("Export process completed successfully");
                    checkpointState.markCompleted();
                } else {
                    logger.info("Export process stopped gracefully before completion");
                }
                checkpointState.save(checkpointFile);
            }

        } catch (Exception e) {
            logger.error("Critical error during export process: {}", e.getMessage(), e);
            checkpointState.setError("Critical error: " + e.getMessage());
            checkpointState.save(checkpointFile);
        }
    }

    private boolean processBatch(List<Map<String, Object>> batch, KafkaProducer kafkaProducer, long batchOffset) {
        logger.debug("Processing batch of {} records from offset {}", batch.size(), batchOffset);
        
        int recordsProcessed = 0;
        int recordsFailed = 0;
        
        for (Map<String, Object> record : batch) {
            try {
                // Generate a key for the record - either use a natural key from the data or generate a UUID
                String key = generateKey(record);
                
                // Send to Kafka
                boolean sent = kafkaProducer.sendRecord(key, record);
                
                if (sent) {
                    recordsProcessed++;
                } else {
                    recordsFailed++;
                }
                
            } catch (InterruptedException e) {
                logger.warn("Interrupted while sending record to Kafka");
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                logger.error("Error processing record: {}", e.getMessage(), e);
                recordsFailed++;
            }
        }
        
        // Flush to ensure all records in this batch are delivered
        kafkaProducer.flush();
        
        logger.debug("Batch processed: {} successful, {} failed", recordsProcessed, recordsFailed);
        
        // Consider batch successful if we processed at least 90% of records
        return recordsFailed <= (batch.size() * 0.1);
    }
    
    private String generateKey(Map<String, Object> record) {
        // Try to use a primary key from the record if available
        // This is just an example - adjust according to your data structure
        if (record.containsKey("id")) {
            return record.get("id").toString();
        }
        // Otherwise generate a random UUID
        return UUID.randomUUID().toString();
    }

    public static void main(String[] args) {
        CassandraToKafkaExporter exporter = new CassandraToKafkaExporter();
        exporter.start();
    }
} 