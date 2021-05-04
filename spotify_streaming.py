from pyspark.sql import SparkSession
from pyspark.sql.functions import *


import time

kafka_topic_name = "songTopic"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("Spotify Streaming Reccomendation System") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    songs_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of songs_df: ")
    songs_df.printSchema()

    songs_df1 = songs_df.selectExpr("CAST(value AS STRING)", "timestamp")


    songs_schema_string = "song_id INT, song_acousticness DOUBLE, song_danceability DOUBLE, song_duration_ms DOUBLE, " \
                           + "song_energy DOUBLE, song_instrumentalness DOUBLE, song_key INT, " \
                           + "song_liveness DOUBLE," \
                           + "song_loudness DOUBLE, song_mode INT, song_speechiness DOUBLE, " \
                           + "song_tempo DOUBLE," \
                           + "song_time_signature DOUBLE," \
                           + "song_valence DOUBLE, song_target INT, song_song_title STRING, " \
                           + "song_artist STRING"



    songs_df2 = songs_df1 \
        .select(from_csv(col("value"), songs_schema_string) \
                .alias("song"), "timestamp")


    songs_df3 = songs_df2.select("song.*", "timestamp")
    songs_df3.printSchema()

    # Write final result into console for debugging purpose
    songs_agg_write_stream = songs_df3 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    songs_agg_write_stream.awaitTermination()

    print("Songs Streaming...")