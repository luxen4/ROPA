from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, when, col, mean # python3 -m pip install numpy
from datetime import datetime


# Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("Sustitución de Nulos en Columna Específica") \
    .getOrCreate()

# Cargar el archivo CSV en un DataFrame de Spark
df_filtrado = spark.read.csv("sales_dataa.csv", header=True, inferSchema=True)


# Sustituir los valores nulos en la columna de fecha con la fecha actual
columna_fecha = "date"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%d-%m-%Y")})


# Sustituir los valores nulos en la columna específica "store_ID" con un valor específico (por ejemplo, 0)
columna_especifica = "store_ID"
valor_reemplazo = 0
df_filtrado = df_filtrado.na.fill({columna_especifica: valor_reemplazo})

# Filtrar los registros donde "product_ID" no es un número
# df_filtrado = df_sustituido.filter(df_sustituido["product_ID"].cast("int").isNotNull())

# Sustituir los valores vacios con un 0
columna_especifica = "product_ID"
valor_reemplazo = 0
df_sustituido = df_filtrado.na.fill({columna_especifica: valor_reemplazo})




# Sustituir los valores vacios con la media
columna_especifica = "quantity_sold"
mean_quantity_sold = df_filtrado.select(mean(col(columna_especifica))).collect()[0][0]
mean_quantity_sold = round(mean_quantity_sold,2)
df_filtrado = df_filtrado.withColumn(columna_especifica, 
                                     when(col(columna_especifica).isNull(), mean_quantity_sold).otherwise(col(columna_especifica)))

# Sustituir los valores vacios con la media
columna_especifica = "revenue"
mean_quantity_sold = df_filtrado.select(mean(col(columna_especifica))).collect()[0][0]
mean_quantity_sold = round(mean_quantity_sold,2)
df_filtrado = df_filtrado.withColumn(columna_especifica, 
                                     when(col(columna_especifica).isNull(), mean_quantity_sold).otherwise(col(columna_especifica)))

# Calcular la media de la columna "revenue"
media_revenue = df_filtrado.select(mean("revenue")).collect()[0][0]

# Reemplazar los valores no numéricos en la columna "revenue" con la media
df_filtrado = df_filtrado.withColumn("revenue",
                            when(col("revenue").cast("float").isNotNull(),
                                 col("revenue")).otherwise(round(media_revenue,2)))


# Guardar el DataFrame resultante como un archivo CSV
df_filtrado.write.csv("saleslimpia.csv", header=True, mode="overwrite")


# Detener la sesión de Spark
spark.stop()




# docker exec -it 03c14ab5a86c23cb866130f502469fcb7113526f0092c91b300207617f566587 /bin/bash   # spark-master1
# cd /opt/spark-apps
# Este archivo se ejecuta desde 
# root@03c14ab5a86c:/opt/spark-apps/AdriFabricarCSV# python prueba.py