#!/bin/bash
# Script to package the Cassandra to Kafka Exporter for distribution

# Exit on any error
set -e

echo "Packaging Cassandra to Kafka Exporter..."

# Create distribution directory
DIST_DIR="dist"
PACKAGE_NAME="cassandra-to-kafka-exporter"
VERSION=$(grep -o '<version>.*</version>' pom.xml | head -1 | sed 's/<version>\(.*\)<\/version>/\1/')
PACKAGE_DIR="${PACKAGE_NAME}-${VERSION}"
FULL_DIST_DIR="${DIST_DIR}/${PACKAGE_DIR}"

# Clean previous distribution if exists
rm -rf "${DIST_DIR}"
mkdir -p "${FULL_DIST_DIR}"

# Build the project
echo "Building project..."
mvn clean package

# Copy files to distribution directory
echo "Copying files to distribution directory..."
cp target/cassandra-to-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar "${FULL_DIST_DIR}/cassandra-to-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar"
cp application.properties.sample "${FULL_DIST_DIR}/"
cp run.sh "${FULL_DIST_DIR}/"
cp run.bat "${FULL_DIST_DIR}/"
cp README.md "${FULL_DIST_DIR}/"
cp INSTALL.md "${FULL_DIST_DIR}/"

# Make scripts executable
chmod +x "${FULL_DIST_DIR}/run.sh"

# Create distribution packages
echo "Creating distribution packages..."
cd "${DIST_DIR}"

# Create ZIP archive
zip -r "${PACKAGE_DIR}.zip" "${PACKAGE_DIR}"

# Create TAR archive
tar -czf "${PACKAGE_DIR}.tar.gz" "${PACKAGE_DIR}"

echo "Distribution packages created:"
echo "  - ${DIST_DIR}/${PACKAGE_DIR}.zip"
echo "  - ${DIST_DIR}/${PACKAGE_DIR}.tar.gz"
echo "Done!" 