#!/bin/bash

echo "Running JUnit tests for Cassandra to Kafka Exporter"
echo "================================================="

# Check if JAVA_HOME is set, or try to find Java
if [ -z "$JAVA_HOME" ]; then
    if command -v java > /dev/null 2>&1; then
        echo "Using Java from PATH"
        JAVA_BIN=java
        JAVAC_BIN=javac
    else
        echo "ERROR: JAVA_HOME environment variable is not set and Java is not in PATH."
        echo "Please set JAVA_HOME to point to your Java installation directory."
        exit 1
    fi
else
    JAVA_BIN="$JAVA_HOME/bin/java"
    JAVAC_BIN="$JAVA_HOME/bin/javac"
fi

# Set classpath for compilation and testing
CLASSPATH="target/classes:target/test-classes"

# Add all dependencies from target/dependency (created during package process)
if [ -d "target/dependency" ]; then
    for jar in target/dependency/*.jar; do
        CLASSPATH="$CLASSPATH:$jar"
    done
else
    echo "Creating dependency directory..."
    mkdir -p target/dependency 2>/dev/null
    
    # Find all dependencies in the local Maven repository based on the pom.xml
    echo "Fetching dependencies..."
    # This part would normally use Maven to fetch dependencies
    echo "NOTE: Normally this would use Maven to fetch dependencies."
    echo "For now, please run the package.sh script first to generate dependencies."
    echo "You can then run this script to execute tests."
fi

# Make sure target directories exist
mkdir -p target/classes 2>/dev/null
mkdir -p target/test-classes 2>/dev/null

# Compile main classes
echo "Compiling main classes..."
$JAVAC_BIN -d target/classes -cp "$CLASSPATH" src/main/java/com/cassandraexport/*.java src/main/java/com/cassandraexport/cassandra/*.java src/main/java/com/cassandraexport/config/*.java src/main/java/com/cassandraexport/kafka/*.java src/main/java/com/cassandraexport/model/*.java

# Compile test classes
echo "Compiling test classes..."
$JAVAC_BIN -d target/test-classes -cp "$CLASSPATH" src/test/java/com/cassandraexport/*.java src/test/java/com/cassandraexport/cassandra/*.java src/test/java/com/cassandraexport/config/*.java src/test/java/com/cassandraexport/kafka/*.java src/test/java/com/cassandraexport/model/*.java

# Copy test resources
echo "Copying test resources..."
cp -f src/test/resources/* target/test-classes/ 2>/dev/null

# Run JUnit tests
echo "Running tests..."
$JAVA_BIN -cp "$CLASSPATH" org.junit.platform.console.ConsoleLauncher --scan-classpath --reports-dir=target/test-reports

echo "================================================="
echo "Test execution complete." 