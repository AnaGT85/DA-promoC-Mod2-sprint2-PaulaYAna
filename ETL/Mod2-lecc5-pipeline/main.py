# importamos todas las librerias que son necesarias para que nuestro código funcione

import requests
import pandas as pd
import numpy as np
import mysql.connector
import soporte as sp
import biblioteca as bb

#abrimos el archivo de tiburones limpio
df_tiburon = pd.read_csv('../../datos/tiburon8.csv', index_col = 0)
print("csv tiburon8 cargado")


print('Sacamos los datos de estos países', bb.dicc_paises.keys())

#Llamamos a la clase Extraccion_union para hacer la llamada a la API
api = sp.Extraccion_union(bb.dicc_paises, df_tiburon)
#Creamos df meteo con los datos de la llamada a la API
df_meteo = api.llamada_API() 
print ("df_meteo creada")

#Filtramos df por los países seleccionados
df_filtrado = api.filtrado(bb.dicc_paises.keys())
print('df filtrado')

#Limpiamos df_meteo
meteo_limpio = api.limpiar_clima(df_meteo)
print('meteo limpio')

#Guardamos el df limpio y unido
api.union_guardado(meteo_limpio, df_filtrado, 'tiburon11.csv')
print('Enhorabuena, has unido los dos df')

#Iniciamos la clase BBdd:
mi_bbdd = sp.Bbdd("tiburones", "AlumnaAdalab")

#Creamos la base de datos
paso1 = mi_bbdd.crear_bbdd()
print('Base de datos tiburones creada')



tabla1 = mi_bbdd.crear_insertar_tabla(bb.tabla_ataques)
print ('Tabla ataques creada')
tabla2 = mi_bbdd.crear_insertar_tabla(bb.tabla_clima)
print ('Tabla clima creada')




for indice, fila in df_tiburon.iterrows(): # itreamos por el dataframe.
    
   
    mi_bbdd.crear_insertar_tabla(bb.query_tiburon)
print("Ya estan los datos insertados en tu tabla ataques")


for indice, fila in df_meteo.iterrows(): # itreamos por el dataframe.
    
   
    mi_bbdd.crear_insertar_tabla(bb.query_clima)
print("Ya estan los datos insertados en tu tabla clima")

