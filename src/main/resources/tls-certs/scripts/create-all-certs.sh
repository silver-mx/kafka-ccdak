#!/bin/bash

# Create a certificate authority (CA) and all certificates for the client and broker(s)
# Call it as "./create-all-certs.sh numBrokers password output-dir"
# Example "./create-all-certs.sh 1 pass123 ./../../tls-certs"

#WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
NUM_BROKERS=$1
PASSWORD=$2
OUTPUT_DIR=$3
CA_DIR="$OUTPUT_DIR/ca"

# Create certificate authority (CA) if it does not exist
test -e "$CA_DIR" || ./create-ca.sh "$CA_DIR"

# Create keystore for the broker(s)
./create-brokers-keystore.sh "$NUM_BROKERS" "$PASSWORD" "$OUTPUT_DIR" "$CA_DIR"

# Create the truststore for the client and broker(s)
./create-client-brokers-truststore.sh "$NUM_BROKERS" "$PASSWORD" "$OUTPUT_DIR" "$CA_DIR"