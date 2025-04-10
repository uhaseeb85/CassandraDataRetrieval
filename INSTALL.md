# Installation Guide: Cassandra to Kafka Exporter

This guide will help you install and run the Cassandra to Kafka Exporter on your system.

## Prerequisites

- Java 8 or higher installed
- Access to a Cassandra cluster
- Access to a Kafka cluster

## Installation Steps

1. **Extract the Distribution Package**

   Extract the ZIP or TAR file to a directory of your choice:
   ```bash
   # For ZIP:
   unzip cassandra-to-kafka-exporter.zip -d /path/to/destination
   
   # For TAR:
   tar -xzf cassandra-to-kafka-exporter.tar.gz -C /path/to/destination
   ```

2. **Configure the Application**

   Copy and edit the sample configuration file:
   ```bash
   cd /path/to/destination
   cp application.properties.sample application.properties
   # Edit application.properties with your preferred text editor
   ```

   **Important settings to configure:**
   - `cassandra.contactPoints`: Your Cassandra host(s)
   - `cassandra.localDatacenter`: Your Cassandra datacenter name
   - `cassandra.keyspace`: The keyspace to read from
   - `cassandra.table`: The table to read from
   - `cassandra.query`: The query to execute (or leave default to read the entire table)
   - `kafka.bootstrapServers`: Your Kafka bootstrap servers
   - `kafka.topic`: The Kafka topic to send data to

3. **Make the Scripts Executable (Linux/Mac only)**

   ```bash
   chmod +x run.sh
   ```

## Running the Application

### On Linux/Mac

```bash
./run.sh
```

Or with a custom configuration file:

```bash
./run.sh my-custom-config.properties
```

### On Windows

```bash
run.bat
```

Or with a custom configuration file:

```bash
run.bat my-custom-config.properties
```

## Monitoring

- Check the logs in the `logs` directory for application status
- The application creates a checkpoint file (`checkpoint.json` by default) that tracks progress

## Troubleshooting

If you encounter any issues:

1. **Check the logs** in the `logs` directory
2. **Verify connectivity** to both Cassandra and Kafka
3. **Ensure correct configuration** in your properties file
4. **Check Java version** by running `java -version` (must be Java 8 or higher)

## Restarting an Export

If the export was interrupted and you want to resume:
- Simply run the application again, it will pick up from the last checkpoint

If you want to start from the beginning:
- Delete the checkpoint file: `rm checkpoint.json` (Linux/Mac) or `del checkpoint.json` (Windows)
- Run the application again

## Advanced Configuration

### JVM Options

You can modify the JVM options in the startup scripts:
- Edit `run.sh` or `run.bat` and change the `JAVA_OPTS` variable

### Custom Logging

You can provide a custom Log4j2 configuration:
1. Create your own `log4j2.xml` file
2. Modify the startup script to include: `-Dlog4j.configurationFile=/path/to/your/log4j2.xml` 