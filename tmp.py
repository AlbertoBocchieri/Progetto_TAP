from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
import requests
import json
import re
from telegram.ext import CommandHandler, Updater

# Configurazioni
KAFKA_BROKER = "localhost:9092"  # Sostituisci con il tuo broker Kafka
TOPIC_NAME = "torrent-topic"
ES_HOST = "http://localhost:9200"  # Sostituisci con il tuo endpoint Elasticsearch
INDEX_NAME = "torrent_data"
TELEGRAM_TOKEN = "7747935597:AAHjm45dio5SauGNyzlsx2YXWoRQxO6SmYQ"
TELEGRAM_CHAT_ID = "221067200"

# Inizializza Elasticsearch
es = Elasticsearch([ES_HOST])

# Inizializza Telegram Bot
bot = Bot(token=TELEGRAM_TOKEN)



def send_telegram_message(bot_token, chat_id, text): 
    api_key = "b279545003f93c2f4a70ed5db82e9284"
    complete_title = text["title"]

    match = re.search(r"\b\d{4}\b", complete_title)
    year = match.group(0) if match else "Anno non trovato"

    match = re.search(r"\b\d{4}\b", complete_title) 
    if match:
        movie_title = complete_title[:match.start()].strip()
    else:
        movie_title = complete_title

    tmdb_url = f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={movie_title}&year={year}"
    try:
        tmdb_response = requests.get(tmdb_url).json()
        if tmdb_response.get("results"):
            poster_path = tmdb_response["results"][0].get("poster_path", "")
            poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}"
            overview = tmdb_response["results"][0].get("overview", "")
            vote_average = tmdb_response["results"][0].get("vote_average", "")
        else:
            poster_url = "https://image.tmdb.org/t/p/w500/gBhLQmpCPoKFMCGsulMbIFzrBID.jpg"
            overview = "Nessuna informazione disponibile"
            vote_average = "Nessuna informazione disponibile"

        if overview != "Nessuna informazione disponibile":
            tmdb_id = tmdb_response["results"][0].get("id", "")
            tmdb_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={api_key}&language=it-IT"
            tmdb_response = requests.get(tmdb_url).json()
            overview = tmdb_response.get("overview", "")
            movie_title = tmdb_response.get("title", "")

        callback_data = f"search_{complete_title}"
        if len(callback_data) > 64:
            callback_data = callback_data[:64]

        keyboard = [
            [{"text": "Scarica Torrent", "callback_data": callback_data}],
            [{"text": "👍", "callback_data": "thumbs_up"}, {"text": "👎", "callback_data": "thumbs_down"}]
        ]
        reply_markup = {"inline_keyboard": keyboard}
        parsedText = f"🎥*Titolo:* {movie_title}\n🎬*Sinossi:* {overview}\n🍿*Voto:* {vote_average}"

        send_photo_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"   
        photo_data = {
            "parse_mode": "Markdown",
            "chat_id": chat_id,
            "photo": poster_url,    
            "caption": parsedText,
            "reply_markup": json.dumps(reply_markup)
        }
        response = requests.post(send_photo_url, data=photo_data)

        if response.status_code == 200:
            print("Messaggio inviato con successo!")
        else:
            print(f"Errore nell'invio del messaggio: {response.status_code}, {response.text}")

    except Exception as e:
        print("Errore durante la ricerca/invio locandina:", e)

def main():
    # Configura il consumer Kafka
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        group_id="torrent_consumer", 
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Decodifica i messaggi come dizionari
    )

    print("Consumer Kafka in esecuzione...")

    for message in consumer:
        torrent = message.value  # Il messaggio Kafka come dizionario
        print(f"Messaggio ricevuto: {torrent['title']}")
    
        print(f"Nuovo torrent trovato: {torrent['title']}")
        result = send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, torrent)
        es.index(index=INDEX_NAME, body=torrent)  # Indicizza il nuovo torrent in Elasticsearch
        

if __name__ == "__main__":
    main()


