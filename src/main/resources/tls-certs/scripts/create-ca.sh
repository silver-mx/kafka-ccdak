#!/bin/bash

WORKING_DIR=$(cd "$(dirname "$0")" && pwd)
CA_CFG_DIR="$WORKING_DIR/ca-cfg"
CA_OUTPUT_DIR="$1"

echo "------------------ CREATE CA [$CA_OUTPUT_DIR] ------------------"

# Create output dir
mkdir "$CA_OUTPUT_DIR"

# Copy the cfg file
cp "$CA_CFG_DIR/ca.cnf" "$CA_OUTPUT_DIR"

# Generate a certificate authority (CA) key and certificate
# NOTE: No password is asked because of '-noenc'
openssl req -x509 \
 -config "$CA_OUTPUT_DIR/ca.cnf" \
 -newkey rsa:4096 \
 -sha512 \
 -noenc \
 -keyout "$CA_OUTPUT_DIR/ca.key" \
 -out "$CA_OUTPUT_DIR/ca.crt"

 # Convert the CA files to a .pem format
 cat "$CA_OUTPUT_DIR/ca.crt" "$CA_OUTPUT_DIR/ca.key" > "$CA_OUTPUT_DIR/ca.pem"