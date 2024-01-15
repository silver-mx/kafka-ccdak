# Confluent Certified Developer for Apache Kafka (CCDAK)

This repository includes several examples and labs taken from:

https://www.pluralsight.com/cloud-guru/courses/confluent-certified-developer-for-apache-kafka-ccdak

plus improvements I have worked on and integration tests to verify the changes.

### HOW TO

#### Test Avro Schema Compatibility

See https://docs.confluent.io/platform/current/schema-registry/develop/maven-plugin.html#schema-registry-test-compatibility

Run `Use schema-registry:test-compatibility`

### TLS

The file `docker-compose.yml` includes the configuration required to enable TLS in the kafka broker. The certificates are placed in `resources/tls-certs` and are generated according to the following guide:

https://developer.confluent.io/courses/security/hands-on-setting-up-encryption/

A script that automates all the steps is available at `resources/tls-certs/scripts/create-brokers-keystore.sh`. It can be used as:

```bash

./create-brokers-keystores.sh [numBrokers] [the-cert-password] [output-dir]

Example: ./create-brokers-keystore.sh 1 pass123 ./..

```

#### Verify TLS

Run `openssl s_client -connect localhost:29094 -tls1_3 -showcerts`

Where `29094` comes from `SSL://localhost:29094` in `docker-compose-yml`:


```
KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker:29092,SSL://localhost:29094,PLAINTEXT_HOST://192.168.1.134:9092'
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