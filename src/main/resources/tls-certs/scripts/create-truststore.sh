#!/usr/bin/env bash

# This script will generate one or more truststores.
# Call it as "./create-truststore.sh [numTrustores] [name] [caDir] [password] [outputDir]"
# Example "./create-truststore.sh 1 broker ./../../tls-certs/ca pass123 ./../../tls-certs"

#WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
NUM_KEYSTORES=$1
ENTITY_NAME=$2
CA_DIR=$3
PASSWORD=$4
OUTPUT_DIR=$5
INDEX_LIST=$(seq 1 1 "$NUM_KEYSTORES")

for i in $INDEX_LIST
do
  # Use name-index format (e.g. broker-1) only if there are more than 1 keystores to create
  NAME="$ENTITY_NAME-$i"
  TRUSTSTORE_DIR="$OUTPUT_DIR/$NAME"
  TRUSTSTORE="$TRUSTSTORE_DIR/truststore-$NAME.pkcs12"

  # Delete the truststore if it already exists
  test -e "$TRUSTSTORE" && rm "$TRUSTSTORE"

  echo "------------------------------- START GENERATING TRUSTSTORE $NAME [$TRUSTSTORE] -------------------------------"

  # Create the folder if it does not exists
  test -e "$TRUSTSTORE_DIR" || mkdir -p "$TRUSTSTORE_DIR"

  # Create a truststore for the broker and import the certificate
  keytool -importcert \
   -storetype PKCS12 \
   -keystore "$TRUSTSTORE" \
   -storepass "$PASSWORD" \
   -alias "ca" \
   -file "$CA_DIR/ca.pem" \
   -noprompt

  # Show truststore content
  keytool -list \
   -storetype PKCS12 \
   -keystore "$TRUSTSTORE" \
   -storepass "$PASSWORD"
   
  test "$?" -eq 0  && echo "KEYSTORE CREATED SUCCESSFULLY..." || echo "THE KEYSTORE COULD NOT BE CREATED..."

  # Save creds
  echo "$PASSWORD" > "$TRUSTSTORE_DIR/truststore-creds-$NAME"
    
  echo "------------------------------- END GENERATING TRUSTSTORE $NAME [$TRUSTSTORE] -------------------------------"

done