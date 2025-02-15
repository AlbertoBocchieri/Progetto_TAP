from bs4 import BeautifulSoup
import requests
import re

TMDB_API_KEY = "b279545003f93c2f4a70ed5db82e9284"

def scrape_site():
    """
    Esegue lo scraping del sito per trovare nuovi torrent.
    """ 
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

            #Uso TMDB per cercare il film e ottenere movie_id e rating
            match = re.search(r"\b\d{4}\b", title)
            if match:
                movie_title = title[:match.start()].strip()
            else:
                movie_title = title

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
        


if __name__ == "__main__":
    print("Avvio scraping...")
    try:
        scrape_site()
        print("Scraping completato!")
    except Exception as e:
        print(f"Errore durante l'esecuzione dello scraping: {e}")
