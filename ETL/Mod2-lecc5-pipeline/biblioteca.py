#

dicc_paises = {"USA" : [-100.445882, 39.7837304] , "Australia" : [134.755 , -24.7761086] , "South Africa" : [24.991639 , -28.8166236] , 
"New Zealand" : [172.8344077, -41.5000831], "Papua New Guinea": [144.2489081, -5.6816069]}

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
