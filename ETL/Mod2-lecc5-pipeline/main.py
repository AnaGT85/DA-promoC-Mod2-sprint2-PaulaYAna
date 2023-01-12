# importamos todas las librerias que son necesarias para que nuestro código funcione

import requests
import pandas as pd
import numpy as np
import mysql.connector
import soporte as sp

#abrimos el archivo de tiburones limpio
df_tiburon = pd.read_csv('../../datos/tiburon8.csv', index_col = 0)
print("csv tiburon8 cargado")

dicc_paises = {"USA" : [-100.445882, 39.7837304] , "Australia" : [134.755 , -24.7761086] , "South Africa" : [24.991639 , -28.8166236] , 
"New Zealand" : [172.8344077, -41.5000831], "Papua New Guinea": [144.2489081, -5.6816069]}
print('Sacamos los datos de estos países', dicc_paises.keys())

#Llamamos a la clase Extraccion_union para hacer la llamada a la API
api = sp.Extraccion_union(dicc_paises, df_tiburon)
#Creamos df meteo con los datos de la llamada a la API
df_meteo = api.llamada_API() 
print ("df_meteo creada")

#Filtramos df por los países seleccionados
df_filtrado = api.filtrado(dicc_paises.keys())
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

#Hacemos la query de tabla_ataques
tabla_ataques = '''
CREATE TABLE IF NOT EXISTS `tiburones`.`ataques` (
  `case_number` VARCHAR(20) NOT NULL ,
  `year`INT NOT NULL,
  `type` VARCHAR(50) NOT NULL,
  `country` VARCHAR(50),
  `activity` VARCHAR (200),
  `injury` TEXT,
  `mes` VARCHAR (10),
  `deceso` VARCHAR(20),
  `genero` VARCHAR (20),
  `especies` VARCHAR (50),
  `edad` FLOAT,
  PRIMARY KEY (`case_number`))
ENGINE = InnoDB;
'''
#Hacemos la query de tabla_clima
tabla_clima = '''
CREATE TABLE IF NOT EXISTS `tiburones`.`clima` (
  `index` INT NOT NULL AUTO_INCREMENT,
  `timepoint` INT,
  `cloudcover` INT, 
  `highcloud` INT, 
  `midcloud` INT, 
  `lowcloud` INT,
  `rh_profile` TEXT, 
  `wind_profile` TEXT, 
  `temp2m` INT, 
  `lifted_index` INT, 
  `rh2m` INT,
  `msl_pressure` INT, 
  `prec_type` VARCHAR (10), 
  `prec_amount` INT, 
  `snow_depth` INT,
  `wind10m_direction` INT, 
  `wind10m_speed` INT, 
  `pais` VARCHAR (20),
  PRIMARY KEY (`index`))
ENGINE = InnoDB;
'''

tabla1 = mi_bbdd.crear_insertar_tabla(tabla_ataques)
print ('Tabla ataques creada')
tabla2 = mi_bbdd.crear_insertar_tabla(tabla_clima)
print ('Tabla clima creada')

query_tiburon = f"""
            INSERT INTO ataques (case_number, year, type, country, activity, injury, mes, deceso, genero, especies, edad) 
            VALUES ( "{fila['case_number']}", "{fila['year']}", "{fila['type']}", "{fila['country']}", "{fila['activity']}", "{fila['injury']}", "{fila['mes']}", "{fila['deceso']}", "{fila['genero']}", "{fila['especies']}", "{fila['edad']}");
            """

query_clima = f"""
            INSERT INTO clima (timepoint, cloudcover, highcloud, midcloud, lowcloud, rh_profile, wind_profile, temp2m, lifted_index, rh2m, msl_pressure, prec_type, 
            prec_amount, snow_depth, wind10m_direction, wind10m_speed, pais) 
            VALUES ("{fila['timepoint']}", "{fila['cloudcover']}", "{fila['highcloud']}", "{fila['midcloud']}", "{fila['lowcloud']}", "{fila['rh_profile']}", "{fila['wind_profile']}", "{fila['temp2m']}", "{fila['lifted_index']}", "{fila['rh2m']}", 
            "{fila['msl_pressure']}", "{fila['prec_type']}", "{fila['prec_amount']}", "{fila['snow_depth']}", "{fila['wind10m.direction']}", "{fila['wind10m.speed']}", "{fila['pais']}");
            """

inserccion1 = mi_bbdd.insertar_datos(query_tiburon, df_tiburon)
print ('Se han insertado tus datos en ataques')
inserccion1 = mi_bbdd.insertar_datos(query_clima, df_meteo)
print ('Se han insertado tus datos en clima')