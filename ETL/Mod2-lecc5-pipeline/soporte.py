# importamos todas las librerias que son necesarias para que nuestro código funcione

import requests
import pandas as pd
import numpy as np
import mysql.connector


# Clase de extracción de datos de 7timer y union de df
class Extraccion_union:
    def __init__(self, dicc_paises, df):
        self.dicc_paises = dicc_paises
        self.df = df

    def llamada_API (self):
        df_meteo = pd.DataFrame()  #Creamos un df vacio para ir añadiendo los resultados de las peticiones de informacion a la API por cada pais
        for k , v in self.dicc_paises.items():   #iteramos por cada key y value de nuestro diccionario
            lon = v[0]   #la longitud sera el valor 0 de nuestra lista de values
            lat= v[1]     #la latitud sera el valor 1 de nuestra lista de values
            url = f'http://www.7timer.info/bin/api.pl?lon=-{lon}&lat={lat}&product=meteo&output=json'    #llamamos a la url de la API
        
            response = requests.get(url=url)
            response.status_code
            response.reason

            meteo = response.json()
            meteo_data = meteo["dataseries"]  #necesitamos la info de la columna dataseries 
            meteo_df = pd.json_normalize(meteo_data)   #generamos un df 
            meteo_df["country"] = k.lower()   #creamos una columna llamada pais 
            df_meteo = pd.concat([df_meteo , meteo_df], axis= 0 , join= 'outer')  #concatenamos cada resultado de cada pais al df vacio
        return df_meteo 

    def filtrado (self, list_paises):
        self.list_paises = list_paises

        df_filtrado = self.df[self.df['country'].isin(self.list_paises)] 
        return df_filtrado


    def limpiar_clima(self, data):
        self.data = data

        try:
            self.data['rh_profile']= self.data['rh_profile'].apply(ast.literal_eval)        
            self.data['wind_profile']= self.data['wind_profile'].apply(ast.literal_eval)

        except:
            rh = self.data['rh_profile'].apply(pd.Series)
            for i in range(len(rh.columns)): # Iteramos por las columnas de rh y definimos el nombre de la columna como rh_+valor de layer y los valores cada value de rh
                nombre = "rh_" + str(rh.iloc[0][i]['layer']) 
                valores = list(rh[i].apply(pd.Series)["rh"] )
                self.data.insert(i, nombre, valores) #Insertamos las columnas en el df clima

            wind = self.data['wind_profile'].apply(pd.Series)
            for i in range(len(wind.columns)): 
                nombre_dir = "wdir_" + str(wind.iloc[0][i]['layer']) 
                valores_dir = list(wind[i].apply(pd.Series)["direction"] )

                nombre_speed = "wspeed_" + str(wind.iloc[0][i]['layer']) 
                valores_speed = list(wind[i].apply(pd.Series)["speed"] )

                data.insert(i, nombre_dir, valores_dir) # Insertamos las columnas generadas en el df de clima
                data.insert(i, nombre_speed, valores_speed)

            data = self.data.groupby('country').mean().reset_index() #Agrupamos el df por la columna country y calculamos la media del resto 
        return data

    def union_guardado(self, data, df_filtrado, nombre_guardado):
        self.data = data
        self.df_filtrado = df_filtrado
        self.nombre_guardado = nombre_guardado

        df_final = self.df_filtrado.merge(self.data, how='left',  on='country') #Unimos los df

        df_final.to_csv(f'../../datos/{self.nombre_guardado}') #Guardamos el df creado
        return df_final.head()


#Clase de creación e insercción de datos en mysql
class Bbdd:

    def __init__ (self, nombre_bbdd, contraseña):
        self.nombre_bbdd = nombre_bbdd
        self.contraseña = contraseña


    def crear_bbdd(self):

        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password=f"{self.contraseña}"
        )
        print("Conexión realizada con éxito")
        
        mycursor = mydb.cursor()

        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.nombre_bbdd};")
            print(mycursor)
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)



    #creamos una funcion para crear tabla nueva
    def crear_insertar_tabla(self, query):
        self.query = query
        
        # nos conectamsos con el servidor usando el conector de sql
        cnx = mysql.connector.connect(user='root', password=f"{self.contraseña}",
                                        host='127.0.0.1', database=f"{self.nombre_bbdd}")
        # iniciamos el cursor
        mycursor = cnx.cursor()
        
        # intentamos hacer la query
        try: 
            mycursor.execute(self.query)
            cnx.commit() 
        # en caso de que podamos ejecutar la query devuelvenos un error para saber en que nos estamos equivocando
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)

    def insertar_datos(self, query, df):
        self.df = df
        self.query = query
        for indice, fila in self.df.iterrows(): # itreamos por el dataframe.
            self.crear_insertar_tabla(self.query)
        print("Ya estan los datos insertados en tu tabla")