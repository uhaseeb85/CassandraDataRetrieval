#!/bin/bash

# Cassandra to Kafka Exporter - Startup Script for Linux/Mac

# Set Java options if needed
JAVA_OPTS="-Xms512m -Xmx2g"

# Check if a custom config file is provided
CONFIG_FILE="application.properties"
if [ $# -gt 0 ]; then
    CONFIG_FILE="$1"
    if [ ! -f "$CONFIG_FILE" ]; then
        echo "Error: Config file $CONFIG_FILE not found!"
        exit 1
    fi
fi

# Copy the sample config file if no config exists
if [ ! -f "$CONFIG_FILE" ] && [ -f "application.properties.sample" ]; then
    echo "No configuration file found. Creating one from sample..."
    cp application.properties.sample "$CONFIG_FILE"
    echo "Please edit $CONFIG_FILE with your connection details before running the application."
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p logs

# Log startup
echo "Starting Cassandra to Kafka Exporter..."
echo "Using configuration: $CONFIG_FILE"
echo "Log files will be written to: $(pwd)/logs/"

# Start the application
java $JAVA_OPTS -jar cassandra-to-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar "$CONFIG_FILE"

# Check exit status
if [ $? -ne 0 ]; then
    echo "Application exited with an error. Please check the logs."
    exit 1
fi 