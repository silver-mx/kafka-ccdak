#!/bin/bash

# This script will generate a keystore for the client.
# Call it as "./create-client-keystore.sh password output-dir ca-dir"
# Example "./create-client-keystore.sh pass123 ./../../tls-certs ./../../tls-certs/ca"

WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
PASSWORD=$1
OUTPUT_DIR="$2/client"
CA_DIR=$3

KEYSTORE="$OUTPUT_DIR/keystore-client.pkcs12"

# First delete the output folder if it exists
test "$OUTPUT_DIR" && rm -rf "$OUTPUT_DIR"

# Create the output folder
mkdir "$OUTPUT_DIR"

# Copy client.cnf file
cp "$WORKING_DIR/client-cfg/client.cnf" "$OUTPUT_DIR"

echo "------------------------------- Client Keystore [$OUTPUT_DIR] -------------------------------"

  # Create key & certificate signing request(.csr file)
  # NOTE: No password is asked because of '-noenc'
  openssl req -new \
  -newkey rsa:4096 \
  -sha512 \
  -noenc \
  -keyout "$OUTPUT_DIR/client.key" \
  -out "$OUTPUT_DIR/client.csr" \
  -config "$OUTPUT_DIR/client.cnf"

  # Sign the certificate with the CA
  openssl x509 -req \
  -sha512 \
  -days 3650 \
  -in "$OUTPUT_DIR/client.csr" \
  -CA "$CA_DIR/ca.crt" \
  -CAkey "$CA_DIR/ca.key" \
  -CAcreateserial \
  -out "$OUTPUT_DIR/client.crt" \
  -extfile "$OUTPUT_DIR/client.cnf" \
  -extensions v3_req

  # .Convert the broker certificate over to pkcs12 format
  openssl pkcs12 -export \
  -in "$OUTPUT_DIR/client.crt" \
  -inkey "$OUTPUT_DIR/client.key" \
  -chain \
  -CAfile "$CA_DIR/ca.pem" \
  -name "client" \
  -out "$OUTPUT_DIR/client.p12" \
  -password "pass:$PASSWORD"

  # Create a keystore for the broker and import the certificate
  keytool -importkeystore \
  -deststorepass "$PASSWORD" \
  -destkeystore "$KEYSTORE" \
  -srckeystore "$OUTPUT_DIR/client.p12" \
  -deststoretype PKCS12  \
  -srcstoretype PKCS12 \
  -noprompt \
  -srcstorepass "$PASSWORD"

  echo "------------------------------- VERIFY KEYSTORE [$KEYSTORE] -------------------------------"

  # Verify the keystore
  keytool -list -v \
      -keystore "$KEYSTORE" \
      -storepass "$PASSWORD"

  # Save creds
  echo "$PASSWORD" > "$OUTPUT_DIR/keystore-creds-client"
