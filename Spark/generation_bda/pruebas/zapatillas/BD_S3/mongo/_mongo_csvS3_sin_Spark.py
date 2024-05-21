# Que lea de mongo y que cree el csv en S3
from pymongo import MongoClient
import csv
#client = MongoClient('mongodb://localhost:27017/') # esta la tiene Rafa

client = MongoClient('mongodb://localhost:27017/?authSource=proyecto')
                     

db = client["proyecto"]
collection = db["zapatillas1"]            # Accede a la colección "ropa"

# Realiza una consulta para encontrar todos los productos de tipo "pantalones"
#consulta = { "style": "HYBRID" }

# Ejecuta la consulta y obtén los resultados
#resultados = collection.find(consulta)
resultados = collection.find()

# Imprime los resultados
data=[]
print("encontrados:")
#print (resultados)
for producto in resultados:
    lista = producto['zapatillas']
    print(lista)
    
    if lista != None:
        for list in lista:
            style = list['style']
            marca= list['marca']
            model= list['model']
            years= list['years']
            print(years)
        
            dic={'style':style, 'marca':marca,'model':model,'years':years}
            print(dic)
            data.append(dic)

        
        
    
    
    # csv
    #df = spark.read.csv("./../../spark-data/csv/habitaciones.csv")
    #ruta_salida = "s3a://my-local-bucket/zapatillasMongo_csv"
    #df=df.write.csv(ruta_salida, mode="overwrite", header=True)
    


# Falta de que haga una consulta'''