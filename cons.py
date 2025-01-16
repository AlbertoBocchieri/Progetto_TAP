from kafka import KafkaConsumer
import json

# Configurazioni
KAFKA_BROKER = "localhost:9092"  # Sostituisci con il tuo broker Kafka
TOPIC_NAME = "torrent-topic"

# Configura il consumer Kafka
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    group_id="torrent_consumer",  # Gruppo di consumo
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Decodifica i messaggi come dizionari
    auto_offset_reset='earliest',  # Avvia dalla prima posizione disponibile
    enable_auto_commit=True,  # Imposta su True per l'autocompletamento dell'offset
    consumer_timeout_ms=1000  # Timeout per evitare di bloccare il consumer
)

print("Consumer Kafka in esecuzione...")

for message in consumer:
    torrent = message.value  # Il messaggio Kafka come dizionario
    print(f"Messaggio ricevuto: {torrent}")
    # Inserisci la logica per la gestione del messaggio, ad esempio l'invio del messaggio Telegram
