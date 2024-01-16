#!/bin/bash

# This script will generate the client and brokers truststores.
# Call it as "./create-client-brokers-truststore.sh numBrokers password output-dir ca-dir"
# Example "./create-client-brokers-truststore.sh 1 pass123 ./../../tls-certs ./../../tls-certs/ca"
#WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
BROKER_ID_LST=$(seq 1 1 "$1")
PASSWORD=$2
TLS_CERTS_OUTPUT_DIR=$3
CA_DIR=$4

function createTruststore() {
    DIR=$1
    TRUSTSTORE="$DIR/$2"
    ENTITY=$3

    # Delete the truststore if it already exists
    test -e "$TRUSTSTORE" && rm "$TRUSTSTORE"

    echo "------------------------------- Truststore [$TRUSTSTORE] -------------------------------"

      # Create a truststore for the broker and import the certificate
      keytool -importcert \
       -storetype PKCS12 \
       -keystore "$TRUSTSTORE" \
       -storepass "$PASSWORD" \
       -alias "ca" \
       -file "$CA_DIR/ca.pem" \
       -noprompt

      echo "------------------------------- Show truststore content [$TRUSTSTORE] -------------------------------"

      # Show truststore content
      keytool -list \
       -storetype PKCS12 \
       -keystore "$TRUSTSTORE" \
       -storepass "$PASSWORD"

      # Save creds
      echo "$PASSWORD" > "$DIR/truststore-creds-${ENTITY}"
}

# Create the truststore for the client, including the dir if it does not exist
CLIENT_DIR="$TLS_CERTS_OUTPUT_DIR/client"
test -e "$CLIENT_DIR" || mkdir "$CLIENT_DIR"
createTruststore "$CLIENT_DIR" "truststore-client.pkcs12" "client"


# Create the truststore for each broker
for i in $BROKER_ID_LST
do
  BROKER="broker-$i"
  OUTPUT_DIR="$TLS_CERTS_OUTPUT_DIR/$BROKER"
  createTruststore "$OUTPUT_DIR" "truststore-$BROKER.pkcs12" "$BROKER"
done