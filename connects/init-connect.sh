#!/bin/sh

echo "Подготовка конфигурации с переменными окружения..."

JSON_PAYLOAD=$(cat <<EOF
{
  "name": "kafka-to-postgres-sink",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "$KAFKA_TOPIC",
    "connection.url": "jdbc:postgresql://$DB_HOST:$DB_PORT/$DB_NAME",
    "connection.username": "$DB_USER",
    "connection.password": "$DB_PASSWORD",
    "table.name.format": "$TARGET_TABLE",
    "insert.mode": "insert",
    "schema.evolution": "basic",
    "primary.key.mode": "none",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "transforms": "hoist",
    "transforms.hoist.type": "org.apache.kafka.connect.transforms.HoistField\$Value",
    "transforms.hoist.field": "payload"
  }
}
EOF
)

echo "Удаление старого коннектора..."
curl -s -X DELETE http://kafka-connect:8083/connectors/kafka-to-postgres-sink

echo -e "\nОтправка новой конфигурации в Kafka Connect..."
curl -s -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d "$JSON_PAYLOAD"

echo -e "\nКоннектор запущен в режиме репликации payload!"
