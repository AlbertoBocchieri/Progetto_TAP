import requests
import re
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
import json
import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Configura Kafka e Elasticsearch
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "torrent-topic"
ES_HOST = "http://localhost:9200"
INDEX_NAME = "torrent_data"
TELEGRAM_TOKEN = ""

# Inizializza il producer Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializza i messaggi come JSON
)

# Inizializza Elasticsearch
es = Elasticsearch([ES_HOST])

# Crea l'indice se non esiste
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(index=INDEX_NAME, mappings={
        "properties": {
            "title": {"type": "text"},
            "magnet_link": {"type": "text"},
        }
    })

# Funzione per controllare se un torrent esiste già in Elasticsearch
def torrent_exists(title):
    """
    Controlla se un torrent esiste già in Elasticsearch basandosi sul titolo completo.
    """
    # Forza l'aggiornamento dell'indice (se necessario per visualizzare subito i dati indicizzati)
    es.indices.refresh(index=INDEX_NAME)

    query = {
        "query": {  
            "match_phrase": {   
                "title": title
            }
        }
    }
    response = es.search(index=INDEX_NAME, body=query)
    return response["hits"]["total"]["value"] > 0

# Funzione per eseguire lo scraping del sito e inviare i nuovi torrent a Kafka
def scrape_site():
    """
    Esegue lo scraping del sito per trovare nuovi torrent.
    """ 
    url = "https://kickasstorrent.cr/user/NAHOM1/"  # Cambia con l'URL reale
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Errore HTTP {response.status_code} durante il caricamento di {url}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    torrents = []

    for item in soup.select(".odd, .even"):  # Cambia il selettore CSS se necessario
        title = re.sub(r'\s+', ' ', item.select_one(".torrentname").text).strip()

        # Salta i torrent già esistenti
        if torrent_exists(title):
            print(f"Torrent già indicizzato: {title}") 
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

        # Manda il messaggio a Kafka
        producer.send(TOPIC_NAME, torrent_data)
        print(f"Inviato a Kafka: {title}")

        torrents.append(torrent_data)

    return torrents

if __name__ == "__main__":
    print("Avvio scraping...")
    while True:
        try:
            torrents = scrape_site()
            if torrents:
                print(f"Trovati {len(torrents)} nuovi torrent.")
            else:
                print("Nessun nuovo torrent trovato.")
        except Exception as e:
            print(f"Errore durante l'esecuzione dello scraping: {e}")

        # Attendi prima di eseguire il prossimo scraping
        time.sleep(300)  # 300 secondi (5 minuti)
