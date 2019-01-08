#==============================================================
# Programa para conectarse a Spark y recibir los tweets, 
# Recibe los tweets y manda al socket el texto del tweet 'limpio'
#==============================================================


import tweepy
import pprint
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import nltk
nltk.download('stopwords')
nltk.download('punkt')
from stop_words import get_stop_words
from nltk.corpus import stopwords
import time

#Claves Twitter
consumer_key = 'wAiOgsZu8811j2Ac3mnyquwiT'
consumer_secret = 'kOT4i7K8OoQnNZoDuYEHrsg5DmAW0TnpVyRWPVWWgr4NiYQmm0'
access_token = '26922451-bGQYatrkw4zQZgl5qwIwO8nQXtIln0ZbScSmp1Rqv'
access_secret = '1ZvfJFdBNSOBqmDmriOXqURGsO5Yudj4s8597LCqe9Wo5'

#Busca los stop words en español y en ingles
#Los stopwords incluyen preposiciones y artículos
stop_words = list(get_stop_words('es'))         #Have around 900 stopwords
nltk_words = list(stopwords.words('english'))   #Have around 150 stopwords
stop_words.extend(nltk_words)

# metodo para limpiar los tweets eliminando caracteres especiales 
# como acentos, ñ y  puntuación
def clean(tweet):
    output = []
    tw = tweet.split(' ')
    for palabra in tw:
        if not palabra in stop_words:
            if "http" not in palabra:
                output.append(palabra)
    pal = ' '.join(output)
    mapping = {'á':'a','é':'e','í':'i','ó':'o','ú':'u',
            'ñ':'n','ñ':'n','#':'' ,'Á':'A','É':'E','Í':'I',
            'Ó':'O','Ú':'U','Ñ':'n',"”":'',"“":'','-': ' ',
            ':':'','@':'','!':'','?':'','"':'',',':'','.':'',
            'RT ':'','The':'',"I'm":'',"'s":'',"i ":'',"\n":'',
            '&amp':'', 'I':'', ';':'', 'A':''}
    for k, v in mapping.items():
        pal = pal.replace(k, v)
    return pal

# metodo que recibe tweets y llama a la función clean para elminar los 
# caracteres especiales
# imprime el tweet original y el "limpio"
class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        i = 0
        try:
            msg = json.loads(data)
            tweet = clean(msg['text'])
            print("=====================================")
            print("ORIGINAL: ",msg['text'])
            print("LIMPIO: ", tweet)
            print("=====================================")
            str_list = list(filter(None, tweet.split(" ")))
            tweet = ' '.join(str_list)
            self.client_socket.send(tweet.encode())
            self.client_socket.send('\n'.encode())

            return True
        except BaseException as e:
            print("DATA ERROR: %s \n" % str(e))
        return True 

#Método para manejar errores
    def on_error(self, status):
        print(status)
        return True

#Metodo que pide los datos de twitter, 
# primero hace la conexion con las llaves y despues filtra con la palabra "trump"
def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['trump'])

#Este es el maim. Llama a los métodos de la clase. 
if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "localhost"      # Get local machine name
  port = 4040                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData(c)
