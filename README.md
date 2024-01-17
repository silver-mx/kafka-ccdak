# Confluent Certified Developer for Apache Kafka (CCDAK)

This repository includes several examples and labs taken from:

https://www.pluralsight.com/cloud-guru/courses/confluent-certified-developer-for-apache-kafka-ccdak

plus improvements I have worked on and integration tests to verify the changes.

### HOW TO

#### Test Avro Schema Compatibility

See https://docs.confluent.io/platform/current/schema-registry/develop/maven-plugin.html#schema-registry-test-compatibility

Run `Use schema-registry:test-compatibility`

### TLS

**Guide:** https://developer.confluent.io/courses/security/hands-on-setting-up-encryption/

An example is provided to test mutual TLS (mTLS). The file `docker-compose.yml` includes the configuration required to enable TLS in the kafka broker.

The folder `resources/tls-certs` contains:

* `ca`: The certificate authority key and certificate used to sign other stores.
* `broker-1`: The keystore and truststore that will be provided in the Kafka broker 1.
* `client`: The truststore required in the application to enable mTLS.

**NOTE:** All the certificates are generated by the script `resources/tls-certs/scripts/create-all-certs.sh`. To use the script do:

```bash
./create-all-certs.sh numBrokers password output-dir encryptedCA

Example: ./create-all-certs.sh 1 pass123 ./../../tls-certs true

```
__NOTE 1__: The given password will be used to access all keystores.

__NOTE 2__: If an encrypted CA is required, the provided password (`pass123`) phrase will be used as the CA encryption pass phrase.

#### Verify TLS

Run `openssl s_client -connect localhost:29094 -tls1_3 -showcerts`

Where `29094` comes from `SSL://localhost:29094` in `docker-compose-yml`:


```
KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker:29092,SSL://localhost:29094,PLAINTEXT_HOST://192.168.1.134:9092'
```

### CLI Utils

The folder `kafka-ccdak/cli-utils` contains some scripts to simplify testing:

 * `consume-tls.sh`: Consumes from a topic using a secure TLS connection.

```bash
Usage: ./consume-tls.sh  [broker-host-port] [topic]
Example: ./consume-tls.sh  localhost:29094 inventory-tls
```

### Kafka Connector

#### Postgresql

Create a Postgresql connector:

`http POST http://dell:8093/connectors/ < connector-data-postgresql.json`

```json
{
    "name": "postgres-accounts-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "192.168.1.134",
        "database.port": 5432,
        "database.user": "postgres",
        "database.password": "password1",
        "database.dbname": "postgres",
        "topic.prefix": "postgres",
        "table.include.list": "public.accounts"
    }
}
```