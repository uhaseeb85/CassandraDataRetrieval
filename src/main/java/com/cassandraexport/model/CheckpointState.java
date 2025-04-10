package com.cassandraexport.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CheckpointState {
    private static final Logger logger = LoggerFactory.getLogger(CheckpointState.class);
    private static final ObjectMapper mapper = new ObjectMapper()
            .configure(SerializationFeature.INDENT_OUTPUT, true);

    private long lastProcessedOffset;
    private int batchesProcessed;
    private long recordsProcessed;
    private String lastProcessedTimestamp;
    private boolean completed;
    private String errorMessage;

    public CheckpointState() {
        this.lastProcessedOffset = 0;
        this.batchesProcessed = 0;
        this.recordsProcessed = 0;
        this.lastProcessedTimestamp = LocalDateTime.now()
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        this.completed = false;
        this.errorMessage = null;
    }

    public static CheckpointState load(String checkpointFilePath) {
        File checkpointFile = new File(checkpointFilePath);
        if (!checkpointFile.exists()) {
            logger.info("No checkpoint file found at {}, creating new checkpoint state", checkpointFilePath);
            return new CheckpointState();
        }

        try {
            CheckpointState state = mapper.readValue(checkpointFile, CheckpointState.class);
            logger.info("Loaded checkpoint: processed {} records, last offset: {}", 
                    state.getRecordsProcessed(), state.getLastProcessedOffset());
            return state;
        } catch (IOException e) {
            logger.error("Failed to load checkpoint file: {}", e.getMessage(), e);
            logger.info("Creating new checkpoint state");
            return new CheckpointState();
        }
    }

    public void save(String checkpointFilePath) {
        try {
            this.lastProcessedTimestamp = LocalDateTime.now()
                    .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            mapper.writeValue(new File(checkpointFilePath), this);
            logger.debug("Saved checkpoint state to: {}", checkpointFilePath);
        } catch (IOException e) {
            logger.error("Failed to save checkpoint: {}", e.getMessage(), e);
        }
    }

    public void updateProgress(long offset, int batchSize) {
        this.lastProcessedOffset = offset;
        this.batchesProcessed++;
        this.recordsProcessed += batchSize;
    }

    public void markCompleted() {
        this.completed = true;
        this.lastProcessedTimestamp = LocalDateTime.now()
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    public void setError(String message) {
        this.errorMessage = message;
        this.lastProcessedTimestamp = LocalDateTime.now()
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    public long getLastProcessedOffset() {
        return lastProcessedOffset;
    }

    public void setLastProcessedOffset(long lastProcessedOffset) {
        this.lastProcessedOffset = lastProcessedOffset;
    }

    public int getBatchesProcessed() {
        return batchesProcessed;
    }

    public void setBatchesProcessed(int batchesProcessed) {
        this.batchesProcessed = batchesProcessed;
    }

    public long getRecordsProcessed() {
        return recordsProcessed;
    }

    public void setRecordsProcessed(long recordsProcessed) {
        this.recordsProcessed = recordsProcessed;
    }

    public String getLastProcessedTimestamp() {
        return lastProcessedTimestamp;
    }

    public void setLastProcessedTimestamp(String lastProcessedTimestamp) {
        this.lastProcessedTimestamp = lastProcessedTimestamp;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
} 