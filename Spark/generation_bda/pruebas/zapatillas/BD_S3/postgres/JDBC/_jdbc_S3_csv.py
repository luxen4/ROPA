# Leer los datos de postgres y crear los csv

from pyspark.sql import SparkSession

# Crear una sesión de Spark
def sesionSpark():
          
    spark = SparkSession.builder \
        .appName("Leer y procesar con Spark") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
        .config("spark.hadoop.fs.s3a.access.key", 'test') \
        .config("spark.hadoop.fs.s3a.secret.key", 'test') \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/hadoop-aws-3.4.0.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark-apps/hadoop-aws-3.4.0.jar") \
        .config("spark.jars","/opt/spark-apps/hadoop-aws-3.4.0.jar") \
        .config("spark.jars","/opt/spark-apps/postgresql-42.7.3.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .master("local[*]") \
        .getOrCreate()
        # .config("spark.jars","./../postgresql-42.7.3.jar") \

    return spark
        
        
def leerPostgres():
    spark = sesionSpark()
        
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/tienda_db"                                                # Desde dentro es en nombre del contenedor y su puerto
    connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}
    
    df = spark.read.jdbc(url=jdbc_url, table="zapatillas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    resultado = spark.sql("SELECT * FROM tabla_spark;")
    resultado.show()
    
    
    resultado \
    .write \
    .option('header', 'true') \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://my-local-bucket/jdbcZapatillas_csv', sep=',')
    


    spark.stop()

leerPostgres()

# resultado.write.csv("s3a://my-local-bucket/hoteles.csv", header=True, mode="overwrite")
# #resultado = spark.sql("SELECT * FROM tabla_spark WHERE store_name ='" + store_name + "';")

