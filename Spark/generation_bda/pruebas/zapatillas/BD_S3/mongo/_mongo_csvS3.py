# Que lea de mongo y que cree el csv en S3
from pymongo import MongoClient
import csv
from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("Leer y procesar con Spark") \
.config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
.config("spark.hadoop.fs.s3a.access.key", 'test') \
.config("spark.hadoop.fs.s3a.secret.key", 'test') \
.config("spark.sql.shuffle.partitions", "4") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
.config("spark.hadoop.fs.s3a.path.style.access", "true") \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
.config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
.config("spark.jars","./postgresql-42.7.3.jar") \
.config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
.master("local[*]") \
.getOrCreate()



# client = MongoClient()                  # Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)
#client = MongoClient('mongodb://root:bda@spark-mongodb-1:27017/')
client = MongoClient('mongodb://root:bda@localhost:27017/proyecto')
                     


db = client["proyecto"]
clients_collection = db["zapatillas"]      # Accede a la colección "clients"



db = client["proyecto"]
collection = db["zapatillas"]            # Accede a la colección "ropa"

# Realiza una consulta para encontrar todos los productos de tipo "pantalones"
#consulta = { "style": "HYBRID" }

# Ejecuta la consulta y obtén los resultados
#resultados = collection.find(consulta)
resultados = collection.find()

# Imprime los resultados
data=[]
print("encontrados:")
print (resultados)
for producto in resultados:
    lista = producto['zapatillas']
    print(lista)
    
    if lista != None:
        for list in lista:
            style = list['style']
            marca= list['marca']
            model= list['model']
            years= list['years']
            print(years)
        
            dic={'style':style, 'marca':marca,'model':model,'years':years}

            data.append(dic)

        df = spark.createDataFrame(data, ["style","marca","model","years"])
        
    
    
    # csv
    #df = spark.read.csv("./../../spark-data/csv/habitaciones.csv")
    ruta_salida = "s3a://my-local-bucket/zapatillasMongo_csv"
    df=df.write.csv(ruta_salida, mode="overwrite", header=True)
    


# Falta de que haga una consulta'''


