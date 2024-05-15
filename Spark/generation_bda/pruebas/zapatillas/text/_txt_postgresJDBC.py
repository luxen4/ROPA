import psycopg2
from pyspark.sql import SparkSession

def read_text_file(filename):
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        
        with open(filename, 'r') as file:
            
            for line in file:
                line = line.strip()
                if len(line) > 0: 
                    style = line.split(',')[0]
                    marca = line.split(',')[1] 
                    model = line.split(',')[2]
                    years = line.split(',')[3]
                    precio= line.split(',')[4]
                    
                    insertar_Zapatillas(style,marca,model,years,precio)
                    
                   

    except FileNotFoundError:
        print(f"File '{filename}' not found.")
        
        
        

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