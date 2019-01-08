
#==============================================================
# Programa para hacer conexion con aplicación de Twitter 
# y extraer los tweets. 
#==============================================================

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

#Claves Twitter
consumer_key = 'wAiOgsZu8811j2Ac3mnyquwiT'
consumer_secret = 'kOT4i7K8OoQnNZoDuYEHrsg5DmAW0TnpVyRWPVWWgr4NiYQmm0'
access_token = '26922451-bGQYatrkw4zQZgl5qwIwO8nQXtIln0ZbScSmp1Rqv'
access_secret = '1ZvfJFdBNSOBqmDmriOXqURGsO5Yudj4s8597LCqe9Wo5'


#La clase TweetsListener extrae los tweets utlizando las  
#llaves de la aplicación de Twitter
class TweetsListener(StreamListener):

    # Inicia el socket
  def __init__(self, csocket):
      self.client_socket = csocket
   
   #metodo para mandar los tweets al puerto deseado con una codificacion utf8
  def on_data(self, data):
      try:
          # El contenido lo carga en formato JSON
          msg = json.loads(data)
          #dic = msg['user']
          # Manda por el socket el campo deseado
          self.client_socket.send(msg['lang'].encode('utf-8'))
          # Truqito
          self.client_socket.send("\n".encode('utf-8'))
          print(data)
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

#Metodo para controlar errores
  def on_error(self, status):
      print(status)
      return True

#Método para autentificar la conexion y empezar el streaming con el filtro deseado
def sendData(c_socket):
  # Conecta con Twitter
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)

  #Parametro de busqueda
  twitterStream = Stream(auth, TweetsListener(c_socket))
  twitterStream.filter(track=['putin'])

#Funcion que manda los datos
if __name__ == "__main__":
  s = socket.socket()         # Crea un socket
  host = "192.168.254.1"      # Mi dirIP
  port = 5555                # El puerto del servicio
  s.bind((host, port))     

  print("Listening on port: %s" % str(port))

  s.listen(5)                 
  c, addr = s.accept()        # Establece conexion con el cliente

  print( "Received request from: " + str( addr ) )

  sendData( c )
