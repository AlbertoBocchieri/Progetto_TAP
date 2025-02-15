import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from sklearn.metrics.pairwise import cosine_similarity

def prepare_dataset(csv_path):
    print(f"Caricamento del dataset da {csv_path}...")
    df = pd.read_csv(csv_path)
    print("Dataset caricato. Creazione della matrice user-item...")
    user_item_matrix = df.pivot_table(index='user_id', columns='movie_id', values='rating', fill_value=0)
    print("Matrice user-item creata.")
    return user_item_matrix

def train_item_based_model(user_item_matrix):
    print("Calcolo della similarità tra i film...")
    similarity_matrix = cosine_similarity(user_item_matrix.T)
    film_ids = user_item_matrix.columns
    print("Similarità calcolata. Creazione del DataFrame delle similarità...")
    sim_df = pd.DataFrame(similarity_matrix, index=film_ids, columns=film_ids)
    print("DataFrame delle similarità creato.")
    return sim_df

def store_model_in_elasticsearch(sim_df, es_index='item_similarity'):
    print(f"Connessione a Elasticsearch e creazione dell'indice '{es_index}' se non esiste...")
    es = Elasticsearch("http://localhost:9200")
    if not es.indices.exists(index=es_index):
        es.indices.create(index=es_index)
        print(f"Indice '{es_index}' creato.")
    else:
        print(f"Indice '{es_index}' già esistente.")
    
    print("Indicizzazione delle similarità per ogni film...")
    for film in sim_df.index:
        similar_items = sim_df.loc[film].to_dict()
        es.index(index=es_index, id=film, body={'similar_items': similar_items})
    print("Indicizzazione completata.")

def recommend_movies_for_user(user_id, user_item_matrix, sim_df, top_n=5):
    print(f"Raccomandazione di film per l'utente {user_id}...")
    user_ratings = user_item_matrix.loc[user_id]
    liked_films = user_ratings[user_ratings == 1].index.tolist()
    scores = pd.Series(0, index=user_item_matrix.columns)
    for film in liked_films:
        scores += sim_df[film]
    scores = scores[user_ratings == 0]
    recommendations = scores.nlargest(top_n).index.tolist()
    print(f"Film raccomandati per l'utente {user_id}: {recommendations}")
    return recommendations

if __name__ == '__main__':

    dataset_csv = 'kickass_dataset.csv'
    print("Preparazione del dataset...")
    user_item_mat = prepare_dataset(dataset_csv)
    print("Addestramento del modello basato sugli item...")
    sim_df = train_item_based_model(user_item_mat)
    print("Memorizzazione del modello in Elasticsearch...")
    store_model_in_elasticsearch(sim_df)
    user_id_test = 123
    consigli = recommend_movies_for_user(user_id_test, user_item_mat, sim_df, top_n=5)
