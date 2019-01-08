#==============================================================
# Programa para conectarse a Spark, recibir los tweets 
# y contar la frequencia de los diferentes idiomas en 
# el cual están escritos los Tweets
#==============================================================


from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession, HiveContext
from pyspark import (SparkConf, SparkContext, SQLContext, Row)

# Actualiza el valor de la suma de ocurrencias
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# Genera una instancia de sqlContext para hacer consultas en cada rdd
def get_sql_context_instance(spark_context):

    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)

    return globals()['sqlContextSingletonInstance']

# Nombre de la aplicacion
conf = SparkConf().setAppName("SparkTwitter")
# Se crea el elemento de configuracion
sc = SparkContext(conf=conf)
# Inicia el streaming
ssc = StreamingContext(sc, 1)
# Crea los checkpoints por si falla o pierde datos
ssc.checkpoint("checkpoint_TwitterApp")

# Carga los datos que obtiene del puerto especifico
hashtag_DS = ssc.socketTextStream("192.168.254.1", 5555)

# Creamos la tabla vacia para añadir los datos
from pyspark.sql.types import *
schema = StructType([])
sql_context = HiveContext(sc)
empty = sql_context.createDataFrame(sc.emptyRDD(), schema)

# método para hacer consultas y guardar los resultados de las consultas para después gráficar
def process_rdd(_, rdd):
    try:
        # Obtiene la instancia de sqlContext para la sesion
        sql_context = get_sql_context_instance(rdd.context)

        # Convierte RDD a DF
        row_rdd = rdd.map(lambda w: Row(idioma=w[0], count=w[1]))
        # Construye el DF a partir de elementos RDD
        lags_df = sql_context.createDataFrame(row_rdd)
        # Guarda el DF como tabla
        lags_df.registerTempTable("idiomas")
        # Consulta SQL
        empty = sql_context.sql(
            "SELECT idioma, count "
            "FROM idiomas "
            "ORDER BY count DESC")
        empty.show() 
    except Exception as e:
        print(e)

#Separa los valores y actualiza las ocurrencias
tags_DS = hashtag_DS.map(lambda word: (word.lower(), 1))\
    .updateStateByKey(aggregate_tags_count)

# Lleva a la funcion de procesamiento anterior
tags_DS.foreachRDD(process_rdd)

ssc.start()

import matplotlib.pyplot as plt
import seaborn as sn
import time
from IPython import display

#Inicia una grafica 
count = 0
while count < 10:
  # El timepo de la grafica es de 40 s
  time.sleep(40)
  top_10_tweets = sql_context.sql(
            "SELECT idioma, count "
            "FROM idiomas "
            "ORDER BY count DESC")
  top_10_df = top_10_tweets.toPandas()
  display.clear_output(wait=True)
  plt.figure( figsize = ( 5, 5 ) )
  sn.barplot( x="idioma", y="count", data=top_10_df)
  #plt.xticks(rotation=90)
  plt.show()
  count = count + 1

ssc.awaitTermination()