#!/bin/bash

# Adds an ACL to allow certain users to read and write to a topic.
# Usage: `./add-user-acl.sh [broker-host-port] [topic].
# Example: `./add-user-acl.sh localhost:29094 acl-test`.

WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
BROKER=$1
TOPIC=$(test "$2" && echo "$2" || echo "acl-test")
ADMIN_CLIENT_CFG="$WORKING_DIR/tls-cfg.properties"

kafka-acls.sh --bootstrap-server "$BROKER" \
  --add \
  --allow-principal User:client-1 \
  --allow-principal User:otheruser \
  --operation read \
  --operation write \
  --topic "$TOPIC" \
  --command-config "$ADMIN_CLIENT_CFG" \
  #--allow-host host-1 \
  #--allow-host host-2 \


