from pyspark.sql import SparkSession


def insertarPostgres():
    spark = SparkSession.builder \
    .appName("ReadFromPostgres") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
    .master("spark://spark-master:7077") \
    .config("spark.jars","postgresql-42.7.3.jar") \
    .getOrCreate()

    df_original = spark.read.csv("./zapatillas.csv", header=True)
    
    df_original.show()
    
    
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/retail_db"
    connection_properties = { "user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    table_name = "zapatillas" 
    # Escribe el DataFrame en la tabla de PostgreSQL
    df_original.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties) # mode="append"


file_name='./zapatillas.csv'
insertarPostgres()