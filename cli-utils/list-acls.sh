#!/usr/bin/env bash

# Lists ACLs for a topic.
# Usage: `./list-acls.sh [broker-host-port] [topic].
# Example: `./list-acls.sh localhost:29094 acl-test`.

WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
BROKER=$1
TOPIC=$(test "$2" && echo "$2" || echo "acl-test")
ADMIN_CLIENT_CFG="$WORKING_DIR/tls-cfg.properties"

kafka-acls.sh --bootstrap-server "$BROKER" \
  --command-config "$ADMIN_CLIENT_CFG" \
  --list \
  --topic "$TOPIC" \
  --resource-pattern-type "match"

