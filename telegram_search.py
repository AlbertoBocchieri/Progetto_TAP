import requests
from bs4 import BeautifulSoup
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import re
from kafka import KafkaProducer
import json

# Configura Kafka e Elasticsearch
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "torrent-topic"
ES_HOST = "http://localhost:9200"
INDEX_NAME = "torrent_data"
TELEGRAM_TOKEN = "7747935597:AAHjm45dio5SauGNyzlsx2YXWoRQxO6SmYQ"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializza i messaggi come JSON
)

# Funzione per gestire il comando /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Ciao! Usa /search seguito dal nome del torrent per cercare un file torrent. 🎥")

# Funzione per cercare i torrent
async def search_torrent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ Per favore, specifica il nome del torrent da cercare!\nEsempio: `/search nome del film`")
        return

    search_query = " ".join(context.args)
    torrents = []
    page = 1
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        while True:
            url = f"https://kickasstorrent.cr/usearch/{search_query} nahom/{page}/"
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                await update.message.reply_text(
                    f"⚠️ Errore HTTP {response.status_code} durante la ricerca di {search_query}."
                )
                return
            
            soup = BeautifulSoup(response.text, "html.parser")
            page_links = soup.select(".odd, .even")

            if not page_links:  # Nessun risultato trovato
                break

            for item in page_links:
                title = re.sub(r'\s+', ' ', item.select_one(".torrentname").text).strip()

                # Salta i torrent già esistenti
                #if torrent_exists(title):
                    #print(f"Torrent già indicizzato: {title}") 
                    #continue    

                # Estrai il link della pagina di dettaglio del torrent
                torrent_page_url = "https://kickasstorrent.cr" + item.select_one(".torrentname a")["href"]

                # Apri la pagina del torrent e prendi il magnet link
                torrent_response = requests.get(torrent_page_url, headers=headers)
                if torrent_response.status_code != 200:
                    print(f"Errore HTTP {torrent_response.status_code} per {torrent_page_url}")
                    continue

                torrent_soup = BeautifulSoup(torrent_response.text, "html.parser")
                magnet_link = torrent_soup.select_one(".kaGiantButton")["href"]
                
                torrent_data = {
                    "title": title,
                    "magnet_link": magnet_link,
                }

                # Manda il messaggio a Kafka
                producer.send(TOPIC_NAME, torrent_data)
                print(f"Inviato a Kafka: {title}")

                torrents.append(torrent_data)
            
            page += 1

        if torrents:
            await update.message.reply_text(
                "🎬 Ecco i risultati trovati:\n\n" + "\n".join(torrents)
            )
        else:
            await update.message.reply_text("⚠️ Nessun risultato trovato.")
    except Exception as e:
        await update.message.reply_text(f"❌ Errore durante la ricerca: {e}")

# Configurazione principale del bot
def main():
    # Crea l'applicazione
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Gestori di comandi
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("search", search_torrent))

    # Avvia il bot con polling
    application.run_polling()

# Esegui il bot
if __name__ == "__main__":
    main()
