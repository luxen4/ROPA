from pyspark.sql import SparkSession                    # python3 -m pip install pyspark 
from pyspark.sql.functions import col                   # python3 -m pip install numpy
from pyspark.ml.feature import Imputer


# Inicializa una sesión de Spark
spark = SparkSession.builder \
    .appName("Tratamiento de Valores Perdidos") \
    .getOrCreate()

# Carga el archivo CSV en un DataFrame
df = spark.read.csv("sample.csv", header=True, inferSchema=True)

# Imprime el esquema del DataFrame
print("Esquema del DataFrame:")
df.printSchema()


# Calcula la media de las columnas numéricas
columnas_numericas = [c[0] for c in df.dtypes if c[1] in ['int', 'double']]
media_columnas_numericas = df.select(*(sum(col(c).isNull().cast('int')).alias(c) for c in columnas_numericas)).first()

# Imputa los valores perdidos con la media de cada columna numérica
imputer = Imputer(strategy='mean', inputCols=columnas_numericas, outputCols=columnas_numericas)
df_imputado = imputer.fit(df).transform(df)

# Muestra el DataFrame con valores imputados
print("DataFrame con valores imputados:")
df_imputado.show()

# Detiene la sesión de Spark
spark.stop()



# docker exec -it 335929619d214460b9f067c85a31056f97ed59154ba2a09b208809a30efc78f2 /bin/bash
# cd /opt/spark-apps
# python prueba.py
# debajo de apps sale el archivo