#!/bin/bash
# Crea il topic se non esiste già
/kafka/bin/kafka-topics.sh --create --topic torrent-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
