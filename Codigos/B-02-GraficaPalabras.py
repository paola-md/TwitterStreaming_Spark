#==============================================================
# Programa para conectarse a Spark, recibir los tweets 
# y contar la frequencia de las diferentes palabras en 
# los tweets y graficar
#==============================================================


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from pyspark.sql import Row, SparkSession

#Crear conexion a spark con el Streaming context local 
# 2 hilos de trabajo y un intervalo de 1 segundo
sc = SparkContext("local[2]", "Twitter_Streaming")
ssc = StreamingContext(sc, 10)
sqlContext = SQLContext(sc)
# checkpoint en caso de errores 
ssc.checkpoint( "file:///Users/Pepe/Desktop/Github/FinalBDNR/Files/checkpoint")

# Sea crea un DStream que se conecta a localhost al puerto 4040
#Recibe los tweets del puerto 4040
socket_stream = ssc.socketTextStream("localhost", 4040)
lines = socket_stream.window(10)

from collections import namedtuple
fields = ("tag", "count" )
Tweet = namedtuple( 'Tweet', fields )

#Map reduce para procesar los tweets
(lines.flatMap( lambda text: text.split(" ")) 
  .map( lambda word: ( word.lower(), 1 ) )
  .reduceByKey( lambda a, b: a + b )
  .map( lambda rec: Tweet( rec[0], rec[1] ) )
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
  .limit(10).registerTempTable("tweets") ) )

ssc.start()


import matplotlib.pyplot as plt
import seaborn as sn
import time
from IPython import display

count = 0
#Grafica la cuenta de tweets
while count < 10:
  time.sleep(40)
  top_10_tweets = sqlContext.sql( 'Select tag, count from tweets  where count > 4' )
  top_10_df = top_10_tweets.toPandas()
  display.clear_output(wait=True)
  plt.figure( figsize = ( 10, 8 ) )
  sn.barplot( x="count", y="tag", data=top_10_df)
  plt.show()
  count = count + 1


ssc.awaitTermination()

