
import googleapiclient.discovery
import googleapiclient.errors
from dotenv import load_dotenv
import random
import os
import time
import requests

LOGSTASH_URL = "http://logstash:5001"

# Carica le variabili d'ambiente dal file .env
load_dotenv()

dev = os.getenv('MY_API_KEY')
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = dev

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=DEVELOPER_KEY)

# Funzione per ottenere canali casuali 
def get_random_channels_with_subscribers():
    # Lista di parole chiave per la ricerca casuale
    keywords = ['tech', 'music', 'news', 'gaming', 'education', 'comedy', 'science', 'travel', 'food']
    keyword = random.choice(keywords)

    try:
        # Esegui la ricerca
        search_request = youtube.search().list(
            q=keyword,
            part='snippet',
            type='channel',
            maxResults=70,
            order='viewCount'  # Ordina per popolarità
        )
        search_response = search_request.execute()

        channels = []

        if 'items' not in search_response:
            print("No items found in search response")
            return []

        for item in search_response['items']:
            channel_id = item['snippet']['channelId']

            # Ottieni dettagli del canale 
            channel_request = youtube.channels().list(
                part='snippet,statistics',
                id=channel_id,
            )
            channel_response = channel_request.execute()

            if 'items' not in channel_response:
                print(f"No items found in channel response for channel ID: {channel_id}")
                continue

            for channel in channel_response['items']:
                channel_info = {
                    'channelId': channel['id'],
                    'channelCountry': channel['snippet'].get('country', 'MissingCountry'),
                    'title': channel['snippet']['title'],
                    'publishedAt': channel['snippet']['publishedAt'].split('T')[0],
                    'subscriberCount': int(channel['statistics']['subscriberCount']),
                    'totalVideo': int(channel['statistics']['videoCount']),
                    'views': int(channel['statistics']['viewCount']),
                }
                channels.append(channel_info)

        return channels

    except googleapiclient.errors.HttpError as e:
        print(f"An HTTP error {e.resp.status} occurred: {e.content}")
        return []

    except KeyError as e:
        print(f"KeyError: {e}")
        return []

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

# Imposta il tempo di attesa tra ogni iterazione (in secondi)
tempo_attesa = 50 

# Esegui la funzione e stampa i risultati
while True:

    random_channels = get_random_channels_with_subscribers()


    for channel in random_channels:
        data = {
            'ChannelID:': channel['channelId'],
            'Title': channel['title'],
            'Country': channel['channelCountry'],
            'Subscribers': channel['subscriberCount'],
            'TotalVideo': channel['totalVideo'],
            'Views': channel['views'],
            'Join_date': channel['publishedAt']
        }

        # Stampo cosa invio a logstash
        print(data)
        try:
            logstash_response = requests.post(LOGSTASH_URL, json=data, timeout=10)
            print("Dati inviati a Logstash con codice di stato:", logstash_response.status_code)
        except requests.exceptions.RequestException as e:
            print("Errore durante l'invio dei dati a Logstash:", e)

    time.sleep(tempo_attesa)
