import tweepy
import pandas as pd
from pysentimiento import create_analyzer
from azure.storage.blob import BlobServiceClient
import json

#Lectura de accesos via archivo json
with open("accesos.json") as file:
   my_file = json.load(file)

tweetpy_json = my_file["tweetpy"]
azureblob_json = my_file["azureblob"]
csv_json = my_file["csv"]

#Llaves de aplicacion
bearer_key = tweetpy_json.get("bearer_key")

#Autenticacion twitter
client = tweepy.Client(bearer_token=bearer_key)

#Parametros de conexion azure blob
storageAccountKey = azureblob_json.get("storageaccountkey")
storageaccountName = azureblob_json.get("storageaccountname")
connectionString = azureblob_json.get("connectionstring")
containerName = azureblob_json.get("containername")
outputBlobName	= csv_json.get("outputBlobName")

#Conexion con la cuenta de almacenamiento en container azure
blob_service_client = BlobServiceClient.from_connection_string(connectionString)
blob_client = blob_service_client.get_blob_client(container=containerName, blob=outputBlobName)

#Extraccion de tweets
tweet_list = []

#Analizadores
analyzer = create_analyzer(task="sentiment", lang="es")
emotion_analyzer = create_analyzer(task="emotion", lang="es")

#BCP
query = "to:BCPComunica -is:retweet"
tweets = tweepy.Paginator(
    client.search_recent_tweets,
    query=query,
    tweet_fields=["id", "author_id",  "created_at", "text"],
    max_results=100).flatten(limit=1000)
    
for tweet in tweets:
    texto = str(tweet.text)
    tweet_row = {"id": "["+str(tweet.id)+"]",
                 "banco": "BCP",
                 "fecha": tweet.created_at,
                 "sentimiento": analyzer.predict(tweet.text).output,
                 "texto": texto.replace('\n',' '),
                 "emocion": emotion_analyzer.predict(tweet.text).output}
    tweet_list.append(tweet_row)

#Scotiabank    
query = "to:ScotiabankPE -is:retweet"
tweets = tweepy.Paginator(
    client.search_recent_tweets,
    query=query,
    tweet_fields=["id", "author_id",  "created_at", "text"],
    max_results=100).flatten(limit=1000)
    
for tweet in tweets:
    texto = str(tweet.text)
    tweet_row = {"id": "["+str(tweet.id)+"]",
                 "banco": "Scotia",
                 "fecha": tweet.created_at,
                 "texto": texto.replace('\n',' '),
                 "sentimiento": analyzer.predict(tweet.text).output,
                 "emocion": emotion_analyzer.predict(tweet.text).output}
    tweet_list.append(tweet_row)

#Interbank
query = "to:interbank -is:retweet"
tweets = tweepy.Paginator(
    client.search_recent_tweets,
    query=query,
    tweet_fields=["id", "author_id",  "created_at", "text"],
    max_results=100).flatten(limit=1000)
    
for tweet in tweets:
    texto = str(tweet.text)
    tweet_row = {"id": "["+str(tweet.id)+"]",
                 "banco": "IBK",
                 "fecha": tweet.created_at,
                 "texto": texto.replace('\n',' '),
                 "sentimiento": analyzer.predict(tweet.text).output,
                 "emocion": emotion_analyzer.predict(tweet.text).output}
    tweet_list.append(tweet_row)
    
#BBVA
query = "to:bbva_peru -is:retweet"
tweets = tweepy.Paginator(
    client.search_recent_tweets,
    query=query,
    tweet_fields=["id", "author_id",  "created_at", "text"],
    max_results=100).flatten(limit=1000)
    
for tweet in tweets:
    texto = str(tweet.text)
    tweet_row = {"id": "["+str(tweet.id)+"]",
                 "banco": "BBVA",
                 "fecha": tweet.created_at,
                 "texto": texto.replace('\n',' '),
                 "sentimiento": analyzer.predict(tweet.text).output,
                 "emocion": emotion_analyzer.predict(tweet.text).output}
    tweet_list.append(tweet_row)

#Generacion de archivo CSV
df = pd.DataFrame(tweet_list)
df.to_csv(outputBlobName, encoding="utf-16", sep="\t", index=False)

with open(outputBlobName, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

