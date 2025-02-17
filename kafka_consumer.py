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

# Funzione per cercare un film nel database Elasticsearch che ritorna True se il film è già presente
def search_in_es(title):
    query = {
        "query": {
            "match": {
                "title": title
            }
        }
    }
    result = es.search(index=INDEX_NAME, body=query)
    return result["hits"]["total"]["value"] > 0

#Funzione per pulire il testo ma mantenere gli spazi invariati
def sanitize_callback_data(text, max_length=64):
    text = re.sub(r"[^a-zA-Z0-9 ]", "", text)
    text = text.replace(" ", " ")
    return text[:max_length]

# Funzione per inviare un messaggio Telegram con locandina, titolo, sinossi e voto del film
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

        complete_title=sanitize_callback_data(complete_title)

        callback_data = f"search_{complete_title}"
        if len(callback_data) > 64:
            callback_data = callback_data[:64]
        print(f"Callback data: {callback_data}")
        thumbs_up_callback = f"thumbs_up|{complete_title[:45]}|{tmdb_id}"
        thumbs_down_callback = f"thumbs_down|{complete_title[:45]}|{tmdb_id}"

        if len(thumbs_up_callback) > 64:
            thumbs_up_callback = thumbs_up_callback[:64]
        if len(thumbs_down_callback) > 64:
            thumbs_down_callback = thumbs_down_callback[:64]

        keyboard = [
            [{"text": "Scarica Torrent", "callback_data": callback_data}],
            [{"text": "👍", "callback_data": thumbs_up_callback}, {"text": "👎", "callback_data": thumbs_down_callback}]
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
        
        result = send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, torrent)

        print(f"Torrent indicizzato: {torrent['title']}")
        es.index(index=INDEX_NAME, body=torrent)  # Indicizza il nuovo torrent in Elasticsearch
        
if __name__ == "__main__":
    main()
