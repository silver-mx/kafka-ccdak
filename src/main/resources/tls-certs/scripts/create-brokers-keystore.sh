#!/bin/bash

# This script will generate certificates for the given number of brokers and set the given password.
# The script is adapted from https://github.com/confluentinc/learn-kafka-courses/blob/main/fund-kafka-security/scripts/keystore-create-kafka-2-3.sh
# Call it as "./create-brokers-keystores.sh numBrokers password output-dir ca-dir"
# Example "./create-brokers-keystore.sh 1 pass123 ./../../tls-certs ./../../tls-certs/ca"

WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
BROKER_ID_LST=$(seq 1 1 "$1")
PASSWORD=$2
TLS_CERTS_OUTPUT_DIR=$3
CA_DIR=$4

for i in $BROKER_ID_LST
do
  BROKER="broker-$i"
  OUTPUT_DIR="$TLS_CERTS_OUTPUT_DIR/$BROKER"
  KEYSTORE="$OUTPUT_DIR/keystore-$BROKER.pkcs12"

  # First delete the output folder if it exists
  test "$OUTPUT_DIR" && rm -rf "$OUTPUT_DIR"

  # Create the output folder
  mkdir "$OUTPUT_DIR"

  # Generate and copy broker cnf file
  cp "$WORKING_DIR/broker-cfg/broker.cnf" "$OUTPUT_DIR"
  sed -i -e "s\broker-#\\$BROKER\g" "$OUTPUT_DIR/broker.cnf"
  mv "$OUTPUT_DIR/broker.cnf" "$OUTPUT_DIR/$BROKER.cnf"

	echo "------------------------------- $BROKER [$OUTPUT_DIR] -------------------------------"

    # Create server key & certificate signing request(.csr file)
    # NOTE: No password is asked because of '-noenc'
    openssl req -new \
    -newkey rsa:4096 \
    -sha512 \
    -noenc \
    -keyout "$OUTPUT_DIR/$BROKER.key" \
    -out "$OUTPUT_DIR/$BROKER.csr" \
    -config "$OUTPUT_DIR/$BROKER.cnf"

    # Sign the broker certificate with the CA
    openssl x509 -req \
    -sha512 \
    -days 3650 \
    -in "$OUTPUT_DIR/$BROKER.csr" \
    -CA "$CA_DIR/ca.crt" \
    -CAkey "$CA_DIR/ca.key" \
    -CAcreateserial \
    -out "$OUTPUT_DIR/$BROKER.crt" \
    -extfile "$OUTPUT_DIR/$BROKER.cnf" \
    -extensions v3_req

    # .Convert the broker certificate over to pkcs12 format
    openssl pkcs12 -export \
    -in "$OUTPUT_DIR/$BROKER.crt" \
    -inkey "$OUTPUT_DIR/$BROKER.key" \
    -chain \
    -CAfile "$CA_DIR/ca.pem" \
    -name "$BROKER" \
    -out "$OUTPUT_DIR/$BROKER.p12" \
    -password "pass:$PASSWORD"

    # Create a keystore for the broker and import the certificate
    keytool -importkeystore \
    -deststorepass "$PASSWORD" \
    -destkeystore "$KEYSTORE" \
    -srckeystore "$OUTPUT_DIR/$BROKER.p12" \
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
    echo "$PASSWORD" > "$OUTPUT_DIR/sslkey-creds-${BROKER}"
    echo "$PASSWORD" > "$OUTPUT_DIR/keystore-creds-${BROKER}"

done