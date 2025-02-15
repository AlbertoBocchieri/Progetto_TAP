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
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime
from packaging.version import Version
from pyspark.sql.types import LongType

qbt_client = Client(host='localhost', port=8080, username='admin', password='adminadmin')
progress_message_id = None
monitor_task = None

# Configurazioni
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "torrent-topic"
ES_HOST = "http://localhost:9200"
INDEX_NAME = "torrent_data"
USER_FEEDBACK_INDEX = "user_feedback"
TELEGRAM_TOKEN = "7747935597:AAHjm45dio5SauGNyzlsx2YXWoRQxO6SmYQ"
TMDB_API_KEY = "b279545003f93c2f4a70ed5db82e9284"

es = Elasticsearch("http://localhost:9200")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Handlers per i comandi Telegram
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Ciao! Usa /search seguito dal nome del torrent per cercare un file torrent. 🎥")

def is_high_quality(title):
    quality_keywords = ["4K", "2160p", "HDR", "BDRemux", "x265"]
    return any(keyword.lower() in title.lower() for keyword in quality_keywords)

async def search_torrent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ Specifica il nome del torrent!\nEsempio: `/search nome del film`")
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
                await update.message.reply_text(f"⚠️ Errore HTTP {response.status_code}")
                return
            
            soup = BeautifulSoup(response.text, "html.parser")
            page_links = soup.select(".odd, .even")

            if not page_links:
                break

            for item in page_links:
                title = re.sub(r'\s+', ' ', item.select_one(".torrentname").text).strip()
                
                if not is_high_quality(title):
                    continue

                torrent_page_url = "https://kickasstorrent.cr" + item.select_one(".torrentname a")["href"]
                torrent_response = requests.get(torrent_page_url, headers=headers)
                
                if torrent_response.status_code != 200:
                    continue

                torrent_soup = BeautifulSoup(torrent_response.text, "html.parser")
                magnet_link = torrent_soup.select_one(".kaGiantButton")["href"]
                
                torrent_data = {
                    "title": title,
                    "magnet_link": magnet_link,
                }

                torrents.append(torrent_data)
                producer.send(TOPIC_NAME, torrent_data)

            page += 1

        if torrents:
            await update.message.reply_text("🎬 Risultati trovati:\n\n" + "\n".join(t["title"] for t in torrents))
        else:
            await update.message.reply_text("⚠️ Nessun risultato trovato.")
    except Exception as e:
        await update.message.reply_text(f"❌ Errore: {e}")

async def search_and_download_torrent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    torrent_name = query.data.split('_')[1]
    response = es.search(index=INDEX_NAME, body={"query": {"match": {"title": torrent_name}}})
    
    if response["hits"]["hits"]:
        magnet_link = response["hits"]["hits"][0]["_source"]["magnet_link"]
        qbt_client.torrents_add(urls=magnet_link)
        await context.bot.send_message(chat_id=query.message.chat.id, text=f"Download di {torrent_name} avviato!")
    else:
        await context.bot.send_message(chat_id=query.message.chat.id, text=f"Nessun risultato per {torrent_name}")

async def get_download_progress(qbt_client):
    try:
        torrents = await asyncio.to_thread(qbt_client.torrents_info)
        progress_report = ""
        keyboard = []
        
        for torrent in torrents:
            progress = torrent.progress * 100
            bar = "▰" * int(progress / 5) + "▱" * (20 - int(progress / 5))
            
            progress_report += (
                f"*Nome:* {torrent.name}\n"
                f"*Progresso:* [{bar}] {progress:.1f}%\n"
                f"*Dimensione:* {torrent.total_size / (1024**3):.2f} GB\n"
                f"*Velocità:* {torrent.dlspeed / 1024 if torrent.dlspeed else 0:.2f} KB/s\n"
                "----------------------------------------\n"
            )
            
            keyboard.append([InlineKeyboardButton(
                f"⏸️/▶️ {torrent.name[:15]}", 
                callback_data=f"stop_{torrent.hash}"
            )])
        
        return progress_report, InlineKeyboardMarkup(keyboard)
    
    except Exception as e:
        return f"Errore: {e}", None

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
                except BadRequest:
                    pass
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        progress_message_id = None
        await update.message.reply_text("Monitoraggio interrotto.")
    except Exception as e:
        await update.message.reply_text(f"Errore: {e}")

async def start_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global monitor_task
    if monitor_task is None or monitor_task.done():
        monitor_task = asyncio.create_task(monitor_downloads(update, context))
        await update.message.reply_text("Monitoraggio avviato!")

async def stop_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global monitor_task
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        monitor_task = None

async def stop_torrent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    torrent_hash = query.data.split('_')[1]
    torrent_info = await asyncio.to_thread(qbt_client.torrents_info, torrent_hashes=torrent_hash)
    
    if torrent_info and torrent_info[0].dlspeed == 0:
        await asyncio.to_thread(qbt_client.torrents_resume, torrent_hashes=torrent_hash)
        await query.edit_message_text(f"▶️ Download ripreso: {torrent_hash}")
    else:
        await asyncio.to_thread(qbt_client.torrents_pause, torrent_hashes=torrent_hash)
        await query.edit_message_text(f"⏸️ Download fermato: {torrent_hash}")

async def refresh_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await monitor_downloads(update, context)

async def thumbs_up(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data_parts = query.data.split("|")
    if len(data_parts) >= 3:
        doc = {
            "user_id": update.effective_user.id,
            "film_name": data_parts[1],
            "tmdb_id": data_parts[2],
            "feedback": 1,
            "timestamp": datetime.now().isoformat()
        }
        es.index(index=USER_FEEDBACK_INDEX, body=doc)

async def thumbs_down(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data_parts = query.data.split("|")
    if len(data_parts) >= 3:
        doc = {
            "user_id": update.effective_user.id,
            "film_name": data_parts[1],
            "tmdb_id": data_parts[2],
            "feedback": -1,
            "timestamp": datetime.now().isoformat()
        }
        es.index(index=USER_FEEDBACK_INDEX, body=doc)


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
    application.add_handler(CallbackQueryHandler(thumbs_up, pattern="^thumbs_up"))
    application.add_handler(CallbackQueryHandler(thumbs_down, pattern="^thumbs_down"))
    #application.add_handler(CommandHandler("consigliami", consigliami))

    # Avvia il bot con polling
    application.run_polling()

# Esegui il bot
if __name__ == "__main__":
    main()
