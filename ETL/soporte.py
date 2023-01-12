# importamos todas las librerias que son necesarias para que nuestro código funcione

import requests
import pandas as pd
import numpy as np
import mysql.connector


#abrimos el archivo de tiburones limpio
df_tiburon = pd.read_csv('../datos/tiburon8.csv', index_col = 0)

#abrimos el archivo del clima
df_clima = pd.read_csv('../datos/meteo_tiburon.csv', index_col= 0)