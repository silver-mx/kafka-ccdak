#!/bin/bash

# Create a certificate authority (CA) and all certificates for the client and broker(s)
# Call it as "./create-all-certs.sh numBrokers password output-dir"
# Example "./create-all-certs.sh 1 pass123 ./../../tls-certs"

#WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
NUM_BROKERS=$(test "$1" || echo "1")
PASSWORD=$(test "$2" || echo "pass123")
OUTPUT_DIR=$(test "$3" || echo "./../../tls-certs")
CA_DIR="$OUTPUT_DIR/ca"

echo "NUM_BROKERS=$NUM_BROKERS"
echo "PASSWORD=$PASSWORD"
echo "OUTPUT_DIR=$OUTPUT_DIR"

# Create certificate authority (CA) if it does not exist
test -e "$CA_DIR" || ./create-ca.sh "$CA_DIR"

# Create keystore for the broker(s)
./create-brokers-keystore.sh "$NUM_BROKERS" "$PASSWORD" "$OUTPUT_DIR" "$CA_DIR"

# Create keystore for the client
./create-client-keystore.sh "$PASSWORD" "$OUTPUT_DIR" "$CA_DIR"

# Create the truststore for the client and broker(s)
./create-client-brokers-truststore.sh "$NUM_BROKERS" "$PASSWORD" "$OUTPUT_DIR" "$CA_DIR"