package com.cassandraexport.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CassandraClientTest {

    @Mock
    private CqlSession mockSession;
    
    @Mock
    private CqlSessionBuilder mockSessionBuilder;
    
    @Mock
    private PreparedStatement mockPreparedStatement;
    
    @Mock
    private BoundStatement mockBoundStatement;
    
    @Mock
    private ResultSet mockResultSet;
    
    @Captor
    private ArgumentCaptor<String> queryCaptor;
    
    private Properties testProperties;
    private CassandraClient cassandraClient;
    
    @BeforeEach
    void setUp() {
        // Make all mocks lenient to avoid unnecessary stubbing warnings
        lenient().when(mockSessionBuilder.withLocalDatacenter(anyString())).thenReturn(mockSessionBuilder);
        lenient().when(mockSessionBuilder.addContactPoint(any(InetSocketAddress.class))).thenReturn(mockSessionBuilder);
        lenient().when(mockSessionBuilder.withAuthCredentials(anyString(), anyString())).thenReturn(mockSessionBuilder);
        lenient().when(mockSessionBuilder.build()).thenReturn(mockSession);
        
        lenient().when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        lenient().when(mockPreparedStatement.bind(anyInt())).thenReturn(mockBoundStatement);
        lenient().when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);
        lenient().when(mockSession.isClosed()).thenReturn(false);
        
        // Set up test configuration
        testProperties = new Properties();
        testProperties.setProperty("cassandra.contactPoints", "localhost");
        testProperties.setProperty("cassandra.port", "9042");
        testProperties.setProperty("cassandra.localDatacenter", "datacenter1");
        testProperties.setProperty("cassandra.keyspace", "testkeyspace");
        testProperties.setProperty("cassandra.table", "testtable");
        testProperties.setProperty("cassandra.query", "SELECT * FROM testkeyspace.testtable");
        
        // Inject test properties
        com.cassandraexport.config.TestAppConfig.injectTestProperties(testProperties);
    }
    
    @Test
    void testFetchBatch() throws Exception {
        // Set up mocks
        try (MockedStatic<CqlSession> mockedCqlSession = Mockito.mockStatic(CqlSession.class)) {
            mockedCqlSession.when(CqlSession::builder).thenReturn(mockSessionBuilder);
            
            // Create test data
            List<Row> rows = createMockRows(5);
            
            // Mock ResultSet
            when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);
            when(mockResultSet.iterator()).thenReturn(rows.iterator());
            
            // Create CassandraClient
            cassandraClient = new CassandraClient();
            
            // Test fetching a batch
            List<Map<String, Object>> results = cassandraClient.fetchBatch(0, 5);
            
            // Verify
            assertNotNull(results);
            assertEquals(5, results.size());
            assertEquals("1", results.get(0).get("id"));
            assertEquals("Test Name 1", results.get(0).get("name"));
        }
    }
    
    @Test
    void testFetchBatchWithOffset() throws Exception {
        // Set up mocks
        try (MockedStatic<CqlSession> mockedCqlSession = Mockito.mockStatic(CqlSession.class)) {
            mockedCqlSession.when(CqlSession::builder).thenReturn(mockSessionBuilder);
            
            // Create test data
            List<Row> rows = createMockRows(5);
            
            // Mock ResultSet
            when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);
            when(mockResultSet.iterator()).thenReturn(rows.iterator());
            
            // Create CassandraClient
            cassandraClient = new CassandraClient();
            
            // Test fetching a batch with offset
            List<Map<String, Object>> results = cassandraClient.fetchBatch(2, 3);
            
            // Verify
            assertNotNull(results);
            assertEquals(2, results.size()); // We expect 2 results based on implementation
        }
    }
    
    @Test
    void testConnectionManagement() throws Exception {
        // Set up mocks
        try (MockedStatic<CqlSession> mockedCqlSession = Mockito.mockStatic(CqlSession.class)) {
            mockedCqlSession.when(CqlSession::builder).thenReturn(mockSessionBuilder);
            
            // Mock isClosed method
            when(mockSession.isClosed()).thenReturn(false);
            
            // Create CassandraClient
            cassandraClient = new CassandraClient();
            
            // Test is connected
            assertTrue(cassandraClient.isConnected());
            
            // Test close
            cassandraClient.close();
            
            // Verify session was closed
            verify(mockSession, times(1)).close();
        }
    }
    
    // Helper method to create mock Cassandra rows
    private List<Row> createMockRows(int count) {
        List<Row> rows = new ArrayList<>();
        
        for (int i = 1; i <= count; i++) {
            Row mockRow = mock(Row.class);
            ColumnDefinitions mockColumnDefs = mock(ColumnDefinitions.class);
            
            // Set up column definitions
            ColumnDefinition idColumnDef = mock(ColumnDefinition.class);
            ColumnDefinition nameColumnDef = mock(ColumnDefinition.class);
            
            // Create mocked CqlIdentifier instances
            com.datastax.oss.driver.api.core.CqlIdentifier idIdentifier = mock(com.datastax.oss.driver.api.core.CqlIdentifier.class);
            com.datastax.oss.driver.api.core.CqlIdentifier nameIdentifier = mock(com.datastax.oss.driver.api.core.CqlIdentifier.class);
            
            // Use lenient stubbing for all mocks created in this helper method
            lenient().when(idIdentifier.asInternal()).thenReturn("id");
            lenient().when(nameIdentifier.asInternal()).thenReturn("name");
            
            lenient().when(idColumnDef.getName()).thenReturn(idIdentifier);
            lenient().when(nameColumnDef.getName()).thenReturn(nameIdentifier);
            
            lenient().when(idColumnDef.getType()).thenReturn(DataTypes.TEXT);
            lenient().when(nameColumnDef.getType()).thenReturn(DataTypes.TEXT);
            
            List<ColumnDefinition> colDefs = List.of(idColumnDef, nameColumnDef);
            lenient().when(mockColumnDefs.iterator()).thenReturn(colDefs.iterator());
            lenient().when(mockColumnDefs.size()).thenReturn(colDefs.size());
            lenient().when(mockColumnDefs.get(0)).thenReturn(idColumnDef);
            lenient().when(mockColumnDefs.get(1)).thenReturn(nameColumnDef);
            
            // Set up row
            lenient().when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefs);
            lenient().when(mockRow.getObject("id")).thenReturn(String.valueOf(i));
            lenient().when(mockRow.getObject("name")).thenReturn("Test Name " + i);
            
            rows.add(mockRow);
        }
        
        return rows;
    }
} 