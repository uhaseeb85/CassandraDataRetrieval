# Cassandra to Kafka Exporter

A robust Java application to export large datasets from Apache Cassandra to Apache Kafka with excellent error handling and recovery capabilities.

## Features

- Export up to 6 million records from Cassandra to Kafka
- Process data in configurable batches (default: 10,000 records per batch)
- Checkpoint-based restart capability to resume from where you left off
- Robust error handling with configurable retry mechanisms
- Configurable Cassandra connection parameters
- Configurable Kafka topic and connection parameters
- Detailed logging with file rotation

## Requirements

- Java 11 or higher
- Maven 3.6.x or higher
- Apache Cassandra (tested with 3.x and 4.x)
- Apache Kafka (tested with 2.x and 3.x)

## Building the Application

1. Clone the repository
2. Build with Maven:

```bash
mvn clean package
```

This will create a JAR file with all dependencies in the `target` directory.

## Configuration

Edit the `application.properties` file in the `src/main/resources` directory before building, or create a custom one in the same directory as the JAR file.

### Cassandra Configuration

```properties
cassandra.contactPoints=localhost
cassandra.port=9042
cassandra.localDatacenter=datacenter1
cassandra.keyspace=my_keyspace
cassandra.table=my_table
cassandra.username=
cassandra.password=

# Query Configuration
cassandra.query=SELECT * FROM my_keyspace.my_table
cassandra.batchSize=10000
cassandra.totalRecords=6000000
```

### Kafka Configuration

```properties
kafka.bootstrapServers=localhost:9092
kafka.topic=cassandra_data
kafka.clientId=cassandra-exporter
kafka.acks=all
kafka.retries=10
kafka.batchSize=16384
kafka.bufferMemory=33554432
```

### Error Handling and State Management

```properties
# State Management
state.checkpointFile=checkpoint.json

# Error Handling
error.maxRetries=5
error.retryBackoffMs=1000
```

## Running the Application

```bash
java -jar target/cassandra-to-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar
```

You can also create a custom properties file and specify it using:

```bash
java -Dlogback.configurationFile=path/to/logback.xml -jar target/cassandra-to-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar path/to/custom-application.properties
```

## Monitoring

The application logs detailed information to both console and file. Log files are stored in the `logs` directory.

## Restarting After Failure

If the application fails or is stopped for any reason, it will automatically resume from the last successful checkpoint when restarted. The progress is tracked in the `checkpoint.json` file (or a custom file specified in the properties).

To restart from the beginning, simply delete the checkpoint file.

## Performance Tuning

- Adjust `cassandra.batchSize` based on your environment. Larger batches may improve throughput but increase memory usage.
- Modify Kafka producer settings like `kafka.batchSize` and `kafka.bufferMemory` to optimize for throughput vs. latency.
- Use a dedicated Cassandra keyspace/table for exports to minimize impact on production workloads.

## Troubleshooting

Check the log files in the `logs` directory for detailed error messages and stack traces.

Common issues:

1. **Connection errors to Cassandra**: Verify contact points, port, and credentials
2. **Connection errors to Kafka**: Verify bootstrap servers and topic existence
3. **Memory issues**: Reduce batch size if experiencing OutOfMemoryError

## License

This project is licensed under the MIT License - see the LICENSE file for details. 