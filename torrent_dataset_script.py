from bs4 import BeautifulSoup
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import requests
import re
import json

TMDB_API_KEY = "b279545003f93c2f4a70ed5db82e9284"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "torrent-topic"
INDEX_NAME = "torrent_data"

es = Elasticsearch("http://localhost:9200")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#Funzione per controllare la qualità del torrent
def is_high_quality(title):
    quality_keywords = ["4K", "2160p", "HDR", "BDRemux", "x265"]
    return any(keyword.lower() in title.lower() for keyword in quality_keywords)

#Funzione per fare scraping su KickassTorrents e salvare i torrent su un file csv
def scrape_site():
    page=1
    #url che parte da pagina 1
    while page <= 10:
        url = f"https://kickasstorrent.cr/usearch/nahom/{page}"  # Cambia con l'URL reale
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Errore HTTP {response.status_code} durante il caricamento di {url}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
    
        for item in soup.select(".odd, .even"):  # Cambia il selettore CSS se necessario
            title = re.sub(r'\s+', ' ', item.select_one(".torrentname").text).strip() 

            if not is_high_quality(title):
                continue

            #Uso TMDB per cercare il film e ottenere movie_id e rating
            match = re.search(r"\b\d{4}\b", title)
            if match:
                movie_title = title[:match.start()].strip()
            else:
                movie_title = title

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

            #Salva il torrent su un altro file csv
            with open('torrents.csv', 'a') as f:
                f.write(f"{torrent_data['title']},{torrent_data['magnet_link']}\n")
            
            tmdb_url = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_API_KEY}&query={movie_title}"
            try:
                tmdb_response = requests.get(tmdb_url).json()
                if tmdb_response.get("results"):
                    movie_id = tmdb_response["results"][0].get("id", "")
                    vote_average = tmdb_response["results"][0].get("vote_average", "")
                else:
                    #Salta il film se non è stato trovato su TMDB
                    print(f"Film non trovato su TMDB: {movie_title}")
                    continue
            except Exception as e:
                print(f"Errore durante la richiesta a TMDB: {e}")
                movie_id = ""
                vote_average = "Nessuna informazione disponibile"

            torrent_data = {
                "user_id": 123,  # Esempio di utente
                "movie_id": movie_id,
                "rating": vote_average
            }

            #Scrivi le informazioni sul file csv
            with open('kickass_dataset.csv', 'a') as f:
                f.write(f"{torrent_data['user_id']},{torrent_data['movie_id']},{torrent_data['rating']}\n")

        page += 1
        
#Funzione per indicizzare i torrent dal file su Elasticsearch
def index_torrents():
    with open("torrents.csv", "r") as f:
        for line in f:
            title, magnet_link = line.strip().split(",")
            doc = {
                "title": title,
                "magnet_link": magnet_link
            }
            es.index(index=INDEX_NAME, body=doc)
            print(f"Torrent indicizzato: {title}")

if __name__ == "__main__":
    print("Avvio scraping...")
    try:
        #scrape_site()
        index_torrents()
        print("Scraping/Indexing completato!")
    except Exception as e:
        print(f"Errore durante l'esecuzione dello scraping: {e}")
