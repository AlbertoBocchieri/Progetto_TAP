import requests
from bs4 import BeautifulSoup
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import re
from qbittorrentapi import Client
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import json
import asyncio
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

qbt_client = Client(host='localhost', port=8080, username='admin', password='adminadmin')
progress_message_id = None

# Configura Kafka e Elasticsearch
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "torrent-topic"
ES_HOST = "http://localhost:9200"
INDEX_NAME = "torrent_data"
TELEGRAM_TOKEN = "7747935597:AAHjm45dio5SauGNyzlsx2YXWoRQxO6SmYQ"

es = Elasticsearch("http://localhost:9200")

GREEN = "\033[92m"   # Verde (puoi usare anche \033[32m)
RED   = "\033[91m"   # Rosso (o \033[31m)
RESET = "\033[0m"    # Resetta il colore

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

async def search_and_download_torrent(update, context):
    query = update.callback_query
    torrent_name = query.data
    print(f"Ricerca del torrent: {torrent_name}")
    response = es.search(index="torrent_data", body={"query": {"match": {"title": torrent_name}}})
    hits = response["hits"]["hits"]
    if hits:
        print(f"Torrent trovato: {hits[0]['_source']['title']}")
        magnet_link = hits[0]["_source"]["magnet_link"]
        qb = Client(host="localhost:8080", username="admin", password="admin")
        qb.auth_log_in()
        qb.torrents_add(urls=magnet_link)

        await context.bot.send_message(chat_id=query.message.chat.id, text="Download di "+torrent_name+" avviato su qBittorrent!")

async def download_progress(update: Update, context: ContextTypes.DEFAULT_TYPE):
            try:
                qb = Client(host="localhost:8080", username="admin", password="admin")
                qb.auth_log_in()
                torrents = qb.torrents_info()
                if not torrents:
                    await update.message.reply_text("Nessun torrent in download attivo.")
                    return

                progress_lines = []
                for torrent in torrents:
                    name = torrent.name if hasattr(torrent, "name") else torrent.get("name", "N/A")
                    progress = getattr(torrent, "progress", 0.0)
                    percent = round(progress * 100, 2)
                    progress_lines.append(f"{name}: {percent}%")
                    
                await update.message.reply_text("Progresso Download:\n" + "\n".join(progress_lines))
            except Exception as e:
                await update.message.reply_text(f"Errore nel recupero dei progressi: {e}")




# Funzione asincrona per ottenere il progresso dei download in tempo reale
async def get_download_progress(qbt_client):
    try:
        # Otteniamo le informazioni sui torrent attivi
        torrents = await asyncio.to_thread(qbt_client.torrents_info)
        progress_report = ""
        
        for torrent in torrents:
            # Recuperiamo le informazioni di base
            name = torrent.name
            progress = torrent.progress * 100  # Progresso come percentuale
            size = torrent.total_size
            num_peers = torrent.num_seeds + torrent.num_leechs

            bar_length = 20  # lunghezza della barra
            filled_length = int(bar_length * torrent.progress)
            # Usa caratteri blocco pieni e leggeri per il riempimento della barra
            bar = "\u2588" * filled_length + "\u2591" * (bar_length - filled_length)

            # Mostriamo il nome e il progresso
            progress_report += f"Nome: {name}\n"
            progress_report += f"Download progress: [{bar}] {torrent.progress * 100:.2f}%\n"
            progress_report += f"Dimensione: {size / (1024 * 1024 * 1024):.2f} GB\n"
            progress_report += f"Peers: {num_peers}\n"
            
            
            
            # Aggiungiamo un controllo sulla velocità di download (se presente)
            dl_speed = torrent.dlspeed
            if dl_speed:
                progress_report += f"Velocità di download: {dl_speed / 1024:.2f} KB/s\n"
            else:
                progress_report += "Velocità di download non disponibile.\n"
                
            progress_report += "-" * 50 + "\n"
        
        return progress_report

    except Exception as e:
        return f"Errore durante il recupero dei dati: {e}"

# Funzione asincrona per monitorare i download e aggiornare periodicamente Telegram
async def monitor_downloads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global progress_message_id
    try:
        while True:
            progress_report = await get_download_progress(qbt_client)
            if progress_message_id is None:
                message = await update.message.reply_text(progress_report)
                progress_message_id = message.message_id
            else:
                await context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=progress_message_id,
                    text=progress_report
                )
            await asyncio.sleep(5)
    except BadRequest as e:
                    # Se l'errore indica che il messaggio non è modificato, lo ignoriamo
                    if "not modified" in str(e):
                        pass
                    else:
                        # Se è un altro tipo di errore, stampalo o gestiscilo diversamente
                        print(f"Errore nell'editing del messaggio: {e}")

# Configurazione principale del bot
def main():
    # Crea l'applicazione
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Gestori di comandi
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("search", search_torrent))
    application.add_handler(CallbackQueryHandler(search_and_download_torrent))
    application.add_handler(CommandHandler("progress", download_progress))
    application.add_handler(CommandHandler("monitor", monitor_downloads))



    # Avvia il bot con polling
    application.run_polling()

# Esegui il bot
if __name__ == "__main__":
    main()
