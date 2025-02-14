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
from telegram.constants import ParseMode
from telegram import BotCommand, Bot, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

qbt_client = Client(host='localhost', port=8080, username='admin', password='adminadmin')
progress_message_id = None
monitor_task = None

# Configura Kafka e Elasticsearch
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "torrent-topic"
ES_HOST = "http://localhost:9200"
INDEX_NAME = "torrent_data"
TELEGRAM_TOKEN = "7747935597:AAHjm45dio5SauGNyzlsx2YXWoRQxO6SmYQ"

es = Elasticsearch("http://localhost:9200")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializza i messaggi come JSON
)

# Funzione per gestire il comando /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Ciao! Usa /search seguito dal nome del torrent per cercare un file torrent. 🎥")

def is_high_quality(title):
    quality_keywords = ["4K", "2160p", "HDR", "BDRemux", "x265"]
    for keyword in quality_keywords:
        if keyword.lower() in title.lower():
            return True
    return False

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
            url = f"https://kickasstorrent.cr/usearch/{search_query} ita nahom/{page}/"
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

                # Salta i torrent di bassa qualità
                if not is_high_quality(title):
                    print(f"Torrent di bassa qualità: {title}")
                    continue

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

                torrents.append(torrent_data) 

                # Manda il messaggio a Kafka
                producer.send(TOPIC_NAME, torrent_data)
                print(f"Inviato a Kafka: {title}")

            page += 1

        if torrents:
            await update.message.reply_text(
                "🎬 Ecco i risultati trovati:\n\n" + "\n".join(torrents)
            )
        else:
            await update.message.reply_text("⚠️ Nessun risultato trovato.")
    except Exception as e:
        await update.message.reply_text(f"❌ Errore durante la ricerca: {e}")

# Funzione che cerca su Elasticsearch e avvia il download del torrent
async def search_and_download_torrent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    torrent_name = query.data.split('_')[1]
    print(f"Ricerca del torrent: {torrent_name}")
    
    response = es.search(index="torrent_data", body={"query": {"match": {"title": torrent_name}}})
    hits = response["hits"]["hits"]
    
    if hits:
        print(f"Torrent trovato: {hits[0]['_source']['title']}")
        magnet_link = hits[0]["_source"]["magnet_link"]
        
        qb = Client(host="localhost:8080", username="admin", password="admin")
        qb.auth_log_in()
        qb.torrents_add(urls=magnet_link)
        
        await context.bot.send_message(chat_id=query.message.chat.id, text=f"Download di {torrent_name} avviato su qBittorrent!")
    else:
        await context.bot.send_message(chat_id=query.message.chat.id, text=f"Nessun risultato trovato per {torrent_name}.")

# Funzione asincrona per ottenere il progresso dei download in tempo reale
async def get_download_progress(qbt_client):
    try:
        # Otteniamo le informazioni sui torrent attivi
        torrents = await asyncio.to_thread(qbt_client.torrents_info)
        progress_report = ""
        keyboard = []
        for torrent in torrents:
            # Recuperiamo le informazioni di base
            name = torrent.name
            size = torrent.total_size
            num_peers = torrent.num_seeds + torrent.num_leechs

            bar_length = 20  # lunghezza della barra
            filled_length = int(bar_length * torrent.progress)
            # Usa caratteri blocco pieni e leggeri per il riempimento della barra
            bar = "\u2588" * filled_length + "\u2591" * (bar_length - filled_length)

            # Mostriamo il nome e il progresso
            progress_report += f"*Nome:* {name}\n"
            progress_report += f"*Download progress:* [{bar}] {torrent.progress * 100:.2f}%\n"
            progress_report += f"*Dimensione:* {size / (1024 * 1024 * 1024):.2f} GB\n"
            progress_report += f"*Peers:* {num_peers}\n"
            
            # Aggiungiamo un controllo sulla velocità di download (se presente)
            dl_speed = torrent.dlspeed
            if dl_speed:
                progress_report += f"Velocità di download: {dl_speed / 1024:.2f} KB/s\n"
            else:
                progress_report += "Velocità di download non disponibile.\n"
                
            stop_button = InlineKeyboardButton(
                f"▶️/⏹️ Riprendi/Ferma {torrent.name[:15]}", 
                callback_data=f"stop_{torrent.hash}"
            )
            markup = InlineKeyboardMarkup(keyboard) if keyboard else None
            keyboard.append([stop_button])
            progress_report += "-" * 50 + "\n"
        
        return progress_report, markup

    except Exception as e:
        return f"Errore durante il recupero dei dati: {e}"

# Funzione asincrona per monitorare i download e aggiornare periodicamente Telegram
async def monitor_downloads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global progress_message_id
    try:
        while True:
            progress_report, markup = await get_download_progress(qbt_client)
            if progress_message_id is None:
                message = await update.message.reply_text(progress_report, reply_markup=markup, parse_mode=ParseMode.MARKDOWN)
                progress_message_id = message.message_id
            else:
                try:
                    await context.bot.edit_message_text(
                        chat_id=update.effective_chat.id,
                        message_id=progress_message_id,
                        text=progress_report,
                        reply_markup=markup,
                        parse_mode=ParseMode.MARKDOWN
                    )
                except BadRequest as e:
                    # Se il messaggio non può essere modificato, lo reinvia
                    if "not modified" in str(e):
                        pass
                    else:
                        print(f"Errore nell'editing del messaggio: {e}")
                        message = await update.message.reply_text(progress_report)
                        progress_message_id = message.message_id
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        # Quando il task viene cancellato, resettiamo progress_message_id
        progress_message_id = None
        await update.message.reply_text("Monitoraggio interrotto.")
    except Exception as e:
        await update.message.reply_text(f"Errore nel monitoraggio: {e}")

# Comando per avviare il monitoraggio (crea il task)
async def start_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global monitor_task
    if monitor_task is None or monitor_task.done():
        monitor_task = asyncio.create_task(monitor_downloads(update, context))
        await update.message.reply_text("Monitoraggio avviato!")
    else:
        await update.message.reply_text("Il monitoraggio è già in funzione.")

# Comando per fermare il monitoraggio (cancella il task)
async def stop_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global monitor_task
    if monitor_task is not None and not monitor_task.done():
        monitor_task.cancel()
        monitor_task = None
    else:
        await update.message.reply_text("Non è attivo nessun monitoraggio.")

# Aggiungi handler per gestire i pulsanti stop
async def stop_torrent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    torrent_hash = query.data.split('_')[1]
    
    torrent_info_list = await asyncio.to_thread(qbt_client.torrents_info, torrent_hashes=torrent_hash)
    if torrent_info_list:
        torrent_info = torrent_info_list[0]
        dl_speed = torrent_info.dlspeed

        # Se la velocità di download è 0, fai partire il download
        if dl_speed == 0:
            await asyncio.to_thread(
                qbt_client.torrents_resume, 
                torrent_hashes=torrent_hash
            )
            await query.edit_message_text(
                text=f"▶️ Download ripreso con successo!\nHash: {torrent_hash}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Aggiorna stato", callback_data="refresh_status")]
                ])
            )
            return

    try:
        await asyncio.to_thread(
            qbt_client.torrents_pause, 
            torrent_hashes=torrent_hash
        )
        await query.edit_message_text(
            text=f"⏹️ Download fermato con successo!\nHash: {torrent_hash}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Aggiorna stato", callback_data="refresh_status")]
            ])
        )
    except Exception as e:
        await query.edit_message_text(f"❌ Errore: {str(e)}")

# Aggiungi handler per l'aggiornamento dello stato
async def refresh_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await monitor_downloads(update, context)
# Configurazione principale del bot
def main():
    # Crea l'applicazione
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Gestori di comandi
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("search", search_torrent))
    application.add_handler(CallbackQueryHandler(search_and_download_torrent, pattern="^search_"))
    application.add_handler(CommandHandler("monitor", start_monitor))
    application.add_handler(CommandHandler("stop_monitor", stop_monitor))
    application.add_handler(CallbackQueryHandler(stop_torrent, pattern="^stop_"))
    application.add_handler(CallbackQueryHandler(refresh_status, pattern="^refresh_status"))

    # Avvia il bot con polling
    application.run_polling()

# Esegui il bot
if __name__ == "__main__":
    main()
