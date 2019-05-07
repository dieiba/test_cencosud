import datetime
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
sc = SparkContext()
SqlContext = SQLContext(sc)
sqlContext = HiveContext(sc)

def inicio():

	file_path = r'/user_dibarra/test_cencosud/retail-food-stores.csv'
	#Generacion de  pandas dataframe y se extrae la latitud y longitud del campo Location con expresiones regulares 
	df_res= df_out(file_path)
	df_res['Latitude']=df_res.Location.str.extract("latitude': '(.*?)'", expand=False)
	df_res['Longitude']=df_res.Location.str.extract("longitude': '(.*?)'", expand=False)
	#Eiminacion del campo Location
	del df_res['Location']
	#Conversion de tipo para evitar problemas en conversion a spark dataframe
	df_res = df_res.astype(str)
	#Eliminacion de espacios
	df_res = df_res.apply(lambda x: x.str.strip())
	#Reemplazo de espacios en nombre columnas
	df_res.columns = df_res.columns.str.replace(" ", "_")
	#Creacion Spark dataframe
	sp_df = sqlContext.createDataFrame(df_res)
	#sp_df.show(30)
	#Guardado de resultado
	#ORC
	sp_df.repartition(1).write.format("orc").mode("overwrite").save('/hdfs_db/user_dibarra/test')
	#CSV
        df_res.to_csv("/user_dibarra/test_cencosud/out.csv",sep=',',encoding='utf-8',index=False)
	print('Fin proceso :'+str(datetime.datetime.now()))


def df_out(path):
	print('Ubicacion archivo:'+path)
	print('Inicio lectura :'+str(datetime.datetime.now()))
	df = pd.read_csv(path)
	print('Fin lectura :'+str(datetime.datetime.now()))
	print('Registros archivo:\n'+str(df.count()))
	return df
	
inicio()	
print('end')
