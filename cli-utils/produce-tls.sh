#!/usr/bin/env bash

# Produces to a topic using a secure TLS connection if needed.
# Usage: `./produce-tls.sh  [broker-host-port] [topic]`.
# Example: `./produce-tls.sh  localhost:29094 inventory-tls`.

WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
BROKER=$1
TOPIC=$2
PRODUCER_CFG="$WORKING_DIR/tls-cfg.properties"

kafka-console-producer.sh \
 --bootstrap-server "$BROKER" \
 --topic "$TOPIC" \
 --producer.config "$PRODUCER_CFG"