from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import pandas as pd
import random

KAFKA_TOPIC_NAME_CONS = "songTopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'



if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    filepath = "data.csv"
    
    songs_df = pd.read_csv(filepath)
    
    #print(songs_df.head(1))
    
    song_list = songs_df.to_dict(orient="records")
    
    for songs in song_list:
        message_list = []
        message_send = None
        message = songs
        message_list.append(message["acousticness"])
        message_list.append(message["danceability"])
        message_list.append(message["duration_ms"])
        message_list.append(message["energy"])
        message_list.append(message["instrumentalness"])
        message_list.append(message["key"])
        message_list.append(message["liveness"])
        message_list.append(message["loudness"])
        message_list.append(message["mode"])
        message_list.append(message["speechiness"])
        message_list.append(message["tempo"])
        message_list.append(message["time_signature"])
        message_list.append(message["valence"])
        message_list.append(message["target"])
        message_list.append(message["song_title"])
        message_list.append(message["artist"])
        message_send = ','.join(str(v) for v in message_list)
        
        print("Message to be sent: ", message_send)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message_send)
        time.sleep(1)