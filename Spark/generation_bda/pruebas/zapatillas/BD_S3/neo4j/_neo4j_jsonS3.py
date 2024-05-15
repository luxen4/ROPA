# Leer de neo4j 
# Hacer el csv en S3
from pyspark.sql import SparkSession      
from neo4j import GraphDatabase

class Neo4jClient:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None

    def connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def get_all_Zapatillas(self, session):
        result = session.run("MATCH (z:Zapatillas) RETURN z")
        return result
    

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
    

    
    # Crear instancia de Neo4jClient
    neo4j_client = Neo4jClient("bolt://neo4j:7687", "neo4j", "your_password")           # Para dentro del cluster
    #neo4j_client = Neo4jClient("bolt://localhost:7687", "neo4j", "your_password")      # Fuera del cluster
    neo4j_client.connect()
    
    # Crear una sesión de Neo4j
    with neo4j_client._driver.session() as session:
        # Consultar todos
        resultados = neo4j_client.get_all_Zapatillas(session)

        data=[]
        for fila in resultados:
            style = fila['z']['marca']  # Aquí puedes hacer algo con cada fila
            marca = fila['z']['marca']
            model = fila['z']['model']
            years = fila['z']['years']
            precio = fila['z']['precio']
            print(precio)
            
            
            linea={'style':style,'marca':marca,'model':model,'years':years,'precio':precio}
    
            data.append(linea)

        
        df = spark.createDataFrame(data, ["style","marca","model","years","precio"])
        
    
        # csv
        #df = spark.read.csv("./../../spark-data/csv/habitaciones.csv")
        ruta_salida = "s3a://my-local-bucket/zapatillasNeo4j_csv"
        df=df.write.csv(ruta_salida, mode="overwrite")
        
    
        
        spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)