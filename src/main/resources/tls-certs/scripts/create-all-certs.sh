#!/bin/bash

# Create a certificate authority (CA) and all certificates for the client and broker(s)
# Call it as "./create-all-certs.sh numBrokers password output-dir encryptedCA"
# Example "./create-all-certs.sh 1 pass123 ./../../tls-certs false"

#WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
NUM_BROKERS=$(test "$1" && echo "$1" || echo "1")
PASSWORD=$(test "$2" && echo "$2" || echo "pass123")
OUTPUT_DIR=$(test "$3" && echo "$3" || echo "./../../tls-certs")
ENCRYPTED=$(test "$4" && echo "$4" || echo "false")
CA_DIR="$OUTPUT_DIR/ca"
CLIENT_SERVER_CFG="./client-server-cfg/config.cnf"

echo "NUM_BROKERS=$NUM_BROKERS"
echo "PASSWORD=$PASSWORD"
echo "OUTPUT_DIR=$OUTPUT_DIR"
echo "CA_DIR=$CA_DIR"
echo "CLIENT_SERVER_CFG=$CLIENT_SERVER_CFG"

# Create certificate authority (CA) if it does not exist
test -e "$CA_DIR" || ./create-ca.sh "$CA_DIR" "$ENCRYPTED"

# Create keystore for the broker(s)
./create-keystore.sh "$NUM_BROKERS" "broker" "$CLIENT_SERVER_CFG" "$CA_DIR" "$PASSWORD" "$OUTPUT_DIR"

# Create the truststore for the broker(s)
./create-truststore.sh "$NUM_BROKERS" "broker" "$CA_DIR" "$PASSWORD" "$OUTPUT_DIR"

# Create keystore for the client
./create-keystore.sh "1" "client" "$CLIENT_SERVER_CFG" "$CA_DIR" "$PASSWORD" "$OUTPUT_DIR"

# Create the truststore for the client
./create-truststore.sh "1" "client" "$CA_DIR" "$PASSWORD" "$OUTPUT_DIR"