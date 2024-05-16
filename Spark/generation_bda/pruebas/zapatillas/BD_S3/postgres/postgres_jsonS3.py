import psycopg2
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
.config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
.master("local[*]") \
.getOrCreate()




def select_zapatillas():
    try:
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="tienda_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        #connection = psycopg2.connect( host="localhost", port="9999", database="primOrd_db", user="primOrd", password="bdaPrimOrd")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()

        create_table_query = """ select * from zapatillas; """
        cursor.execute(create_table_query)
        
        
            # Obtener los resultados
        results = cursor.fetchall()
        return results
        
        
        
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)

results = select_zapatillas()


data=[]
# Imprimir los resultados
for row in results:
    style=row[0]
    marca=row[1]
    model=row[2]
    years=row[3]
    precio=row[4]
    
    item={"style":style,"marca":marca,"model":model,"years":years,"precio":precio}
    data.append(item)
    
    
df = spark.createDataFrame(data)



# json
#df = spark.read.option("multiline", "true").json("./../../spark-data/json/restaurantes.json")
ruta_salida = "s3a://my-local-bucket/zapatillasPostgres_json"
df.write.option("multiline", "true").json(ruta_salida, mode="overwrite")
