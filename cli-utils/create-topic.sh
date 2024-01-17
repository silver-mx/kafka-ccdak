#!/bin/bash

# Creates a topic.
# Usage: `./create-topic.sh  [broker-host-port] [topic] [partitions]`.
# Example: `./create-topic.sh  localhost:29094 my-topic 2`.

WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
BROKER=$1
TOPIC=$2
PARTITIONS=$(test "$3" && echo "$3" || echo "2")
ADMIN_CLIENT_CFG="$WORKING_DIR/tls-cfg.properties"

kafka-topics.sh \
 --bootstrap-server "$BROKER" \
 --create \
 --topic "$TOPIC" \
 --partitions "$PARTITIONS" \
 --replication-factor 1 \
 --command-config "$ADMIN_CLIENT_CFG"