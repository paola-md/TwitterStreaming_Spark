
#==============================================================
# Programa para conectarse a Spark, recibir los hashtags
# y contar la frequencia de los diferentes hashtags y graficar
#==============================================================


import logging
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession, HiveContext
from pyspark import (SparkConf, SparkContext, SQLContext, Row)


#El log para manejar los erroes
logging.getLogger().setLevel(
    level=logging.ERROR
)

#Crear conexion a spark con el Streaming context local 
# 2 hilos de trabajo y un intervalo de 1 segundo
conf = SparkConf().setMaster("local[2]").setAppName("SparkTwitter")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 1) #Aqui se especifica el intervalo de espera entre trabajos
# checkpoint en caso de errores 
ssc.checkpoint("checkpoint_TwitterApp")

# Sea crea un DStream que se conecta a localhost al puerto 4040
hashtag_DS = ssc.socketTextStream("localhost", 4040) 


#Metodo para contar la frecuencia de los hashtags
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

#Metodo para obtener una instancia de SQL
def get_sql_context_instance(spark_context):

    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)

    return globals()['sqlContextSingletonInstance']


from pyspark.sql.types import *
schema = StructType([])
sql_context = HiveContext(sc)
empty = sql_context.createDataFrame(sc.emptyRDD(), schema)

# Metodo para obtener los resultados de la cuenta
def process_rdd(_, rdd):
    try:
        
		#Obtiene el singleton de sql 
        sql_context = get_sql_context_instance(rdd.context)

		#convierte de RDD a Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # Crea Dataframe de Row DD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register dataframe como tabla
        hashtags_df.registerTempTable("hashtags")
        # obtiene los 20 hashtags con mayor frecuencia
        empty = sql_context.sql(
            "SELECT hashtag, hashtag_count "
            "FROM hashtags "
            "ORDER BY hashtag_count "
            "DESC "
            "LIMIT 20"
        )
        empty.show() 
    except Exception as e:
        logging.error(e)

#Metodo de map reduce
tags_DS = hashtag_DS.map(lambda word: (word.lower(), 1))\
    .updateStateByKey(aggregate_tags_count)

# do processing for each RDD generated in each interval
tags_DS.foreachRDD(process_rdd)

ssc.start()

import matplotlib.pyplot as plt
import seaborn as sn
import time
from IPython import display

#Grafica los hashtags con mayor frecuencia
count = 0
while count < 10:
  time.sleep(40)
  top_10_tweets = sql_context.sql(
            "SELECT hashtag, hashtag_count "
            "FROM hashtags "
            "ORDER BY hashtag_count "
            "DESC "
            "LIMIT 20"
        )
  top_10_df = top_10_tweets.toPandas()
  display.clear_output(wait=True)
  plt.figure( figsize = ( 10, 5 ) )
  sn.barplot( x="hashtag", y="hashtag_count", data=top_10_df)
  plt.xticks(rotation=25)
  plt.show()
  count = count + 1



ssc.awaitTermination()
