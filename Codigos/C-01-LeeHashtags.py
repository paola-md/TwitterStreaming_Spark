#==============================================================
# Programa para conectarse a Spark y recibir los tweets, 
# Recibe los tweets y manda al socket el los hashtags del tweet
#==============================================================

import json
import logging
import socket
import requests
import requests_oauthlib

#Claves Twitter
consumer_key = 'wAiOgsZu8811j2Ac3mnyquwiT'
consumer_secret = 'kOT4i7K8OoQnNZoDuYEHrsg5DmAW0TnpVyRWPVWWgr4NiYQmm0'
access_token = '26922451-bGQYatrkw4zQZgl5qwIwO8nQXtIln0ZbScSmp1Rqv'
access_secret = '1ZvfJFdBNSOBqmDmriOXqURGsO5Yudj4s8597LCqe9Wo5'

#Auxiliar para la conexion
auth = requests_oauthlib.OAuth1(consumer_key,
                                consumer_secret,
                                access_token,
                                access_secret,
                                signature_type='auth_header')


#Metodo para obtener un filtro de tweets
def get_tweets_filter(track='', locations: tuple = ()):
    # streaming requests doc:
    # http://docs.python-requests.org/en/master/user/advanced/#streaming-requests

    url = 'https://stream.twitter.com/1.1/statuses/filter.json?'
    data = f'track={track}'
    query_url = url + data

    response = requests.get(query_url, auth=auth, stream=True)

    return response

#Metodo para mandar sólo hashtags de los tweets
def send_hashtags_data_server(response, connection):

    for line in response.iter_lines():
        if line:
            decoded_line = line.decode('utf-8')
            tweet_json = json.loads(decoded_line)
            # tweet_text = tweet_json['text']

            tweet_hashtags = tweet_json['entities']['hashtags']
            if tweet_hashtags:
                for hashtag in tweet_hashtags:

                    hashtag_text = hashtag['text']
                    connection.send(hashtag_text.encode('utf-8') + b'\n')
                    logging.info(f'Sent hashtag:\n{hashtag_text}\n=======')


#Metodo para crear conexion al puerto 
def create_data_server_connection():

    host = 'localhost'
    port = 4040

    # make a TCP socket object
    socket_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # bind it to server port number
    socket_obj.bind((host, port))
    socket_obj.listen()

    logging.info("Waiting for TCP connection...")

    return socket_obj.accept()


#Metodo que crea la conexion
if __name__ == '__main__':

    # set up logging
    logging.getLogger().setLevel(
        level=logging.INFO
    )

    # create data server connection
    connection, address = create_data_server_connection()

    logging.info("Connected. Receiving tweets.")
    tweet_stream = get_tweets_filter(track='FelizJueves')
    send_hashtags_data_server(tweet_stream, connection)
