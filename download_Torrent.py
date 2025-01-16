import requests
import re
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
import os

# Configura Elasticsearch
ES_HOST = "http://host.docker.internal:9200"
INDEX_NAME = "torrent_data"

es = Elasticsearch([ES_HOST])

# Crea l'indice se non esiste
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(index=INDEX_NAME, mappings={
        "properties": {
            "title": {"type": "text"},
            #"category": {"type": "keyword"},
            "magnet_link": {"type": "text"},
            #"description": {"type": "text"},
            #"torrent_file": {"type": "text"}  # Aggiungi un campo per il file torrent
        }
    })

def scrape_site():
    url = "https://kickasstorrent.cr/user/NAHOM1/"  # Cambia con l'URL reale
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Errore HTTP {response.status_code} durante il caricamento di {url}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    torrents = []
    for item in soup.select(".odd, .even"):  # Cambia il selettore CSS
        title = re.sub(r'\s+', ' ', item.select_one(".torrentname").text).strip()
        #category = re.sub(r'\s+', ' ', item.select_one(".torrent-category").text).strip()
        #magnet_link = item.select_one(".torrent-magnet")["href"]
        #description = re.sub(r'\s+', ' ', item.select_one(".torrent-description").text).strip()

        # Estrai il link della pagina di dettaglio del torrent
        torrent_page_url ="https://kickasstorrent.cr"+item.select_one(".torrentname a")["href"]

        # Apri la pagina del torrent e prendi il magnet link
        torrent_response = requests.get(torrent_page_url, headers=headers) 
        torrent_soup = BeautifulSoup(torrent_response.text, "html.parser")
        magnet_link = torrent_soup.select_one(".kaGiantButton ")["href"]

        torrents.append({
            "title": title,
            #"category": category,
            "magnet_link": magnet_link,
            #"description": description,
            #"torrent_file": torrent_file_path
        })

    return torrents

def index_torrents(torrents):
    for torrent in torrents:
        es.index(index=INDEX_NAME, body=torrent)
        print(f"Indicizzato: {torrent['title'] + ' ' + torrent['magnet_link']}")

if __name__ == "__main__":
    print("Avvio scraping...")
    torrent_data = scrape_site()
    if torrent_data:
        index_torrents(torrent_data)
        print("Dati indicizzati con successo.")
    else:
        print("Nessun dato trovato.")
