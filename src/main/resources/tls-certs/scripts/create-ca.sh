#!/bin/bash

# Create a certificate authority (CA) key and certificate
# Call it as "./create-ca.sh output-dir encrypted"
# Example "./create-ca.sh ./../../tls-certs/ca false"

WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
CA_CFG_DIR="$WORKING_DIR/ca-cfg"
CA_OUTPUT_DIR="$1"
ENCRYPTED=$(test "$2" && echo "$2" || echo "false")

# Create output dir
test -e "$CA_OUTPUT_DIR" && echo "CA FOLDER EXISTS, ABORTING..." && exit 1 || mkdir "$CA_OUTPUT_DIR"

# Copy the cfg file
cp "$CA_CFG_DIR/ca.cnf" "$CA_OUTPUT_DIR"

# Generate a certificate authority (CA) key and certificate
if [ "$ENCRYPTED" = "true" ]
then
  echo "------------------------------- START GENERATING ENCRYPTED-CA [$CA_OUTPUT_DIR] -------------------------------"
  # NOTE: Encrypt using '-aes256'
  openssl req -x509 \
   -aes256 \
   -config "$CA_OUTPUT_DIR/ca.cnf" \
   -newkey rsa:4096 \
   -sha512 \
   -keyout "$CA_OUTPUT_DIR/ca.key" \
   -out "$CA_OUTPUT_DIR/ca.crt"
else
  echo "------------------------------- START GENERATING CA [$CA_OUTPUT_DIR] -------------------------------"
  # NOTE: No password is asked because of '-noenc'
  openssl req -x509 \
   -noenc \
   -config "$CA_OUTPUT_DIR/ca.cnf" \
   -newkey rsa:4096 \
   -sha512 \
   -keyout "$CA_OUTPUT_DIR/ca.key" \
   -out "$CA_OUTPUT_DIR/ca.crt"
fi

test "$?" -eq 0  && echo "CA CREATED SUCCESSFULLY..." || echo "THE CA COULD NOT BE CREATED..."

# Convert the CA files to a .pem format
cat "$CA_OUTPUT_DIR/ca.crt" "$CA_OUTPUT_DIR/ca.key" > "$CA_OUTPUT_DIR/ca.pem"

echo "------------------------------- END GENERATING CA [$CA_OUTPUT_DIR] -------------------------------"