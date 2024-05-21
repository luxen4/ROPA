from pymongo import MongoClient
import usosjson

client = MongoClient()                  # Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)
#db = client["proyecto"]
#clients_collection = db["zapatillas1"]      # Accede a la colección "clients"


#filename="./zapatillas.json"
#data = usosjson.read_json_file(filename)



#clients_collection.insert_one({"zapatillas1": data}) # Inserta una lista
#print("Zapatillas insertados con exito.")




db = client["proyecto"]
collection = db["zapatillas1"]            # Accede a la colección "ropa"

# Realiza una consulta para encontrar determinados
#consulta = { "style": "KO" }
#resultados = collection.find(consulta)

resultados = collection.find()

print("encontrados:")
for item in resultados:
    zapa = item['zapatillas1']
    for zap in zapa:
        print(zap['style'])


# Falta de que haga una consulta'''

















'''
#filename='./../data_Prim_ord/json/cligentes.json'
filename='./../../../../data_bda/json/clientes.json'
clients = read_json_file(filename)

clients_collection.insert_one({"clients": clients}) # Inserta la lista de clients


print("Contenido de la colección 'clients':")       # Imprimir
for clients in clients_collection.find():
    print(clients)'''
    
