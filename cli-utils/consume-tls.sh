#!/bin/bash

# Consumes from a topic using a secure TLS connection if needed.
# Usage: `./consume-tls.sh  [broker-host-port] [topic]`.
# Example: `./consume-tls.sh  localhost:29094 inventory-tls`.

WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
BROKER=$1
TOPIC=$2
CONSUMER_CFG="$WORKING_DIR/tls-cfg.properties"

kafka-console-consumer.sh \
 --bootstrap-server "$BROKER" \
 --topic "$TOPIC" \
 --from-beginning \
 --consumer.config "$CONSUMER_CFG"