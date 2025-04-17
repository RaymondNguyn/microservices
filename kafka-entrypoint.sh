#!/bin/bash
# Custom entrypoint for Kafka that removes meta.properties if it exists

# Path to meta.properties - adjust if needed for your Kafka setup
META_PROPERTIES_PATH="/kafka/kafka-logs/meta.properties"

# Check if file exists and remove it
if [ -f "$META_PROPERTIES_PATH" ]; then
  echo "Found meta.properties file, removing it..."
  rm -f "$META_PROPERTIES_PATH"
  echo "meta.properties removed successfully"
else
  echo "meta.properties file not found, continuing..."
fi

# Execute the original entrypoint with all arguments
exec "$@"