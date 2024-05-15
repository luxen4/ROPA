from pyspark.sql import SparkSession


# Crear la SparkSession
def sesion_Spark():
     
    spark = SparkSession.builder \
    .appName("SPARK S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", 'test') \
    .config("spark.hadoop.fs.s3a.secret.key", 'test') \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
    
    return spark


def leerConSpark():
    spark = sesion_Spark()

    try:
        #df = spark.read.text("s3a://my-local-bucket/dataPokemon.json")
        #df = spark.read.option("header", "true").csv("s3a://my-local-bucket/zapatillas_csv")
        df = spark.read.json("s3a://my-local-bucket/zapatillasNeo4j_json")
        
        df.show()
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)

leerConSpark()

