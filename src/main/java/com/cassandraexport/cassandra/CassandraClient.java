package com.cassandraexport.cassandra;

import com.cassandraexport.config.AppConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CassandraClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CassandraClient.class);
    private final AppConfig config;
    private CqlSession session;
    private PreparedStatement pagingStatement;

    public CassandraClient() {
        this.config = AppConfig.getInstance();
        initializeSession();
    }

    private void initializeSession() {
        logger.info("Initializing Cassandra connection to {} on port {}", 
                config.getCassandraContactPoints(), config.getCassandraPort());
        
        try {
            CqlSessionBuilder sessionBuilder = CqlSession.builder()
                    .withLocalDatacenter(config.getCassandraLocalDatacenter());
            
            // Add contact points
            String[] contactPoints = config.getCassandraContactPoints().split(",");
            for (String contactPoint : contactPoints) {
                sessionBuilder.addContactPoint(
                        new InetSocketAddress(contactPoint.trim(), config.getCassandraPort())
                );
            }
            
            // Add credentials if provided
            if (StringUtils.isNotBlank(config.getCassandraUsername()) && 
                StringUtils.isNotBlank(config.getCassandraPassword())) {
                sessionBuilder.withAuthCredentials(
                        config.getCassandraUsername(),
                        config.getCassandraPassword()
                );
            }
            
            session = sessionBuilder.build();
            logger.info("Successfully connected to Cassandra cluster");

            // Prepare paging statement (custom query or a generic one based on keyspace and table)
            String query = config.getCassandraQuery();
            if (StringUtils.isBlank(query)) {
                query = String.format("SELECT * FROM %s.%s", 
                    config.getCassandraKeyspace(), config.getCassandraTable());
            }
            
            // Append a LIMIT if there isn't one already
            if (!query.toLowerCase().contains(" limit ")) {
                query += " LIMIT ?";
                this.pagingStatement = session.prepare(query);
                logger.info("Prepared query: {}", query);
            } else {
                throw new IllegalArgumentException("Please provide a query without LIMIT clause, " +
                        "as it will be added automatically for pagination");
            }
            
        } catch (Exception e) {
            logger.error("Failed to initialize Cassandra connection: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize Cassandra connection", e);
        }
    }

    public List<Map<String, Object>> fetchBatch(long offset, int batchSize) {
        logger.debug("Fetching batch of {} records from offset {}", batchSize, offset);
        
        List<Map<String, Object>> results = new ArrayList<>(batchSize);
        AtomicInteger count = new AtomicInteger(0);
        
        try {
            // Create a paging query with the current offset and batch size
            String query = String.format("%s ALLOW FILTERING", 
                    config.getCassandraQuery().replace(" LIMIT ?", ""));
            
            ResultSet resultSet;
            if (offset > 0) {
                // Skip to the correct offset
                BoundStatement boundStatement = pagingStatement.bind(batchSize);
                resultSet = session.execute(boundStatement);
                
                // Skip records until we reach the offset
                long toSkip = offset;
                for (Row row : resultSet) {
                    if (toSkip-- <= 0) {
                        break;
                    }
                }
            } else {
                BoundStatement boundStatement = pagingStatement.bind(batchSize);
                resultSet = session.execute(boundStatement);
            }
            
            // Process results
            for (Row row : resultSet) {
                if (count.get() >= batchSize) {
                    break;
                }
                
                // Convert Row to Map
                Map<String, Object> rowMap = new HashMap<>();
                for (ColumnDefinition columnDef : row.getColumnDefinitions()) {
                    String columnName = columnDef.getName().asInternal();
                    Object value = row.getObject(columnName);
                    
                    // Handle arrays and collections appropriately
                    if (value != null && value.getClass().isArray()) {
                        if (value instanceof byte[]) {
                            value = Arrays.toString((byte[]) value);
                        } else {
                            value = Arrays.toString((Object[]) value);
                        }
                    }
                    
                    rowMap.put(columnName, value);
                }
                
                results.add(rowMap);
                count.incrementAndGet();
            }
            
            logger.debug("Fetched {} records", count.get());
            return results;
            
        } catch (Exception e) {
            logger.error("Error fetching data batch from Cassandra: {}", e.getMessage(), e);
            throw new RuntimeException("Error fetching data batch from Cassandra", e);
        }
    }

    public boolean isConnected() {
        return session != null && !session.isClosed();
    }

    @Override
    public void close() {
        if (session != null && !session.isClosed()) {
            try {
                session.close();
                logger.info("Cassandra session closed");
            } catch (Exception e) {
                logger.error("Error closing Cassandra session: {}", e.getMessage());
            }
        }
    }
} 