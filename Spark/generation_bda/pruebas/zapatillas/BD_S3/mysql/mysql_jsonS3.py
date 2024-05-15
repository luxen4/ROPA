from pyspark.sql import SparkSession
import mysql.connector


def selectTable():
    try:
        conexion = mysql.connector.connect( host="mysql",user="root",password="alberite",database="tienda_db")
        cursor = conexion.cursor()

        sql = """Select * from zapatillas; """

        cursor.execute(sql)
         # Obtener los resultados
        resultados = cursor.fetchall()
        
        print("Datos OK.")
        return resultados
        
    except Exception:
        #print(f"File '{filename}' not found.")
        print("No se ha podido crear la tabla.")


try:
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
    
    
    resultados = selectTable()
    data=[]
    for fila in resultados:
        print(fila)  # Aquí puedes hacer algo con cada fila
        
        linea={'style':fila[0],'marca':fila[1],'model':fila[2],'years':fila[3],'precio':fila[3]}
        data.append(linea)
        
    df = spark.createDataFrame(data, ["style","marca","model","years","precio"])

    # json
    #df = spark.read.option("multiline", "true").json("./../../spark-data/json/restaurantes.json")
    ruta_salida = "s3a://my-local-bucket/zapatillas_json"
    df.write.option("multiline", "true").json(ruta_salida, mode="overwrite")
    
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)