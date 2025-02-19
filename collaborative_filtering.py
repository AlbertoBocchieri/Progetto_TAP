from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql.functions import col

def prepare_dataset(csv_path):
    spark = SparkSession.builder \
        .appName("MovieRecommendation") \
        .getOrCreate()
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    return df, spark

def train_model(df):
    als = ALS(
        userCol="user_id",
        itemCol="movie_id",
        ratingCol="rating",
        coldStartStrategy="drop"
    )
    model = als.fit(df)
    return model

def save_model(model, path):
    # Salva il modello, sovrascrivendo se già esistente
    model.write().overwrite().save(path)
    print(f"Modello salvato in {path}")

def load_model(spark, path):
    model = ALSModel.load(path)
    print(f"Modello caricato da {path}")
    return model

def recommend_movies_for_user(model, user_id, top_n=5):
    user_df = model.userFactors.sparkSession.createDataFrame([(user_id,)], ["user_id"])
    recs_df = model.recommendForUserSubset(user_df, top_n)
    recs = recs_df.collect()
    if recs:
        recommended_ids = [rec.movie_id for rec in recs[0]["recommendations"]]
        return recommended_ids
    return []


if __name__ == "__main__":
    csv_path = "kickass_dataset2.csv"  # Il dataset dei film: user_id, movie_id, rating
    model_save_path = "models2/als_model"  # Directory dove salvare il modello

    df, spark = prepare_dataset(csv_path)
    
    try:
        model = load_model(spark, model_save_path)
    except Exception as e:
        print("Modello non trovato, addestramento in corso...")
        model = train_model(df)
        save_model(model, model_save_path)
    
    user_id = 123  # Esempio di utente
    recommendations = recommend_movies_for_user(model, user_id, top_n=5)
    print(f"Raccomandazioni per l'utente {user_id}: {recommendations}")
    
    spark.stop()
