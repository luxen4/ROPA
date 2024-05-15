
from pyspark.sql import SparkSession

def insertarPostgres(file_name):
    spark = SparkSession.builder \
    .appName("ReadFromPostgres") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
    .master("spark://spark-master:7077") \
    .config("spark.jars","postgresql-42.7.3.jar") \
    .getOrCreate()

    df_original = spark.read.json(file_name)
    
    df_original.show()
    
    
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/retail_db"
    connection_properties = { "user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    table_name = "zapatillas" 
    # Escribe el DataFrame en la tabla de PostgreSQL
    df_original.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties) # mode="append"


file_name='./zapatillas.json'
insertarPostgres(file_name)