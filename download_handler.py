import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from qbittorrentapi import Client
from elasticsearch import Elasticsearch
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

TELEGRAM_TOKEN = "7747935597:AAHjm45dio5SauGNyzlsx2YXWoRQxO6SmYQ"

logging.basicConfig(level=logging.INFO)
es = Elasticsearch("http://localhost:9200")

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

def main() -> None:
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CallbackQueryHandler(search_and_download_torrent))
    application.run_polling()

if __name__ == "__main__":
    main()
