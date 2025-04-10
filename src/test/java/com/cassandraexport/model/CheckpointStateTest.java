package com.cassandraexport.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

public class CheckpointStateTest {

    private CheckpointState checkpointState;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() {
        checkpointState = new CheckpointState();
    }
    
    @Test
    void testInitialState() {
        assertEquals(0, checkpointState.getLastProcessedOffset());
        assertEquals(0, checkpointState.getBatchesProcessed());
        assertEquals(0, checkpointState.getRecordsProcessed());
        assertFalse(checkpointState.isCompleted());
        assertNull(checkpointState.getErrorMessage());
    }
    
    @Test
    void testUpdateProgress() {
        long offset = 100;
        int batchSize = 10;
        
        checkpointState.updateProgress(offset, batchSize);
        
        assertEquals(offset, checkpointState.getLastProcessedOffset());
        assertEquals(1, checkpointState.getBatchesProcessed());
        assertEquals(batchSize, checkpointState.getRecordsProcessed());
    }
    
    @Test
    void testMarkCompleted() {
        checkpointState.markCompleted();
        
        assertTrue(checkpointState.isCompleted());
    }
    
    @Test
    void testSetError() {
        String errorMessage = "Test error message";
        
        checkpointState.setError(errorMessage);
        
        assertEquals(errorMessage, checkpointState.getErrorMessage());
    }
    
    @Test
    void testSaveAndLoad() throws IOException {
        // Set up checkpoint state with some data
        checkpointState.updateProgress(100, 10);
        checkpointState.updateProgress(200, 20);
        
        // Save to temp file
        File tempFile = tempDir.resolve("checkpoint-test.json").toFile();
        String checkpointPath = tempFile.getAbsolutePath();
        checkpointState.save(checkpointPath);
        
        // Load from file
        CheckpointState loadedState = CheckpointState.load(checkpointPath);
        
        // Verify loaded state matches original
        assertEquals(checkpointState.getLastProcessedOffset(), loadedState.getLastProcessedOffset());
        assertEquals(checkpointState.getBatchesProcessed(), loadedState.getBatchesProcessed());
        assertEquals(checkpointState.getRecordsProcessed(), loadedState.getRecordsProcessed());
        assertEquals(checkpointState.isCompleted(), loadedState.isCompleted());
        assertEquals(checkpointState.getErrorMessage(), loadedState.getErrorMessage());
    }
    
    @Test
    void testLoadNonExistentFile() {
        File tempFile = tempDir.resolve("nonexistent.json").toFile();
        String nonExistentPath = tempFile.getAbsolutePath();
        
        CheckpointState loadedState = CheckpointState.load(nonExistentPath);
        
        // Should create a new checkpoint state with initial values
        assertEquals(0, loadedState.getLastProcessedOffset());
        assertEquals(0, loadedState.getBatchesProcessed());
        assertEquals(0, loadedState.getRecordsProcessed());
        assertFalse(loadedState.isCompleted());
        assertNull(loadedState.getErrorMessage());
    }
} 