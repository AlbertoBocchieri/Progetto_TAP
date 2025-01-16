from telegram import Update
from telegram.ext import Updater, CallbackQueryHandler, CommandHandler
import subprocess

TELEGRAM_TOKEN = "7747935597:AAHjm45dio5SauGNyzlsx2YXWoRQxO6SmYQ"

def start_download(magnet_link):
    """
    Scarica il torrent usando un client torrent (ad esempio, Transmission).
    """
    subprocess.run(["transmission-cli", magnet_link])

def handle_download(update: Update, context):
    """
    Gestisce il comando di download inviato tramite Telegram.
    """
    query = update.callback_query
    query.answer()

    # Ottieni il magnet link dal callback_data
    _, magnet_link = query.data.split('|') 

    # Avvia il download
    start_download(magnet_link)

    # Conferma all'utente
    query.edit_message_text(text="Download avviato!")

# Configura il bot per ascoltare i comandi
updater = Updater(token=TELEGRAM_TOKEN)
dispatcher = updater.dispatcher

# Callback per il download
dispatcher.add_handler(CallbackQueryHandler(handle_download))

# Avvia il bot
updater.start_polling()
updater.idle()
