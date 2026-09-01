#!/bin/bash
# scripts/check_kafka_topics.sh
# Liệt kê topic hiện có trong Kafka — chạy để verify Kafka sống và nhận lệnh

echo ">> Danh sách Kafka topics:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo ">> Test tạo topic thử 'test-topic' (sẽ dùng ở Giai đoạn B thật sự):"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic test-topic \
  --partitions 1 \
  --replication-factor 1

echo ""
echo ">> Danh sách topics sau khi tạo:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
