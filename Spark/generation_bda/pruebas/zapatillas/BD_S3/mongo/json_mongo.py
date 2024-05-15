from pymongo import MongoClient
import json

#client = MongoClient()                  # Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)
client = MongoClient('mongodb://root:bda@spark-mongodb-1:27017/proyecto')
db = client["proyecto"]
clients_collection = db["zapatillas"]      # Accede a la colección "clients"


# Leer un json
def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            #print(data)
            return data
    except FileNotFoundError:
        return None



# Que lea un .txt
zapatillas=[]
def read_text_file(filename):
    try:
       
        with open(filename, 'r') as file:
            
            for line in file:
                line = line.strip()
                if len(line) > 0:  
                    style = line.split(',')[0]
                    marca = line.split(',')[1] 
                    model = line.split(',')[2]
                    years = line.split(',')[3]
                    precio= line.split(',')[4]
                    
                    zapatilla = {"style":style, "marca":marca, "model": model, "years":years, "precio":precio}
                    zapatillas.append(zapatilla)
                
                    clients_collection.insert_one({"zapatillas": zapatillas}) # Inserta la lista de clients
                    print("Zapatillas insertados con exito.")


    except FileNotFoundError:
        print(f"File '{filename}' not found.")



filename="./zapatillas.json"
data = read_json_file(filename)

#print(data)

clients_collection.insert_one({"zapatillas": data}) # Inserta la lista de clients
print("Zapatillas insertados con exito.")







'''
db = client["proyecto"]
collection = db["zapatillas"]            # Accede a la colección "ropa"

# Realiza una consulta para encontrar todos los productos de tipo "pantalones"
#consulta = { "style": "HYBRID" }

# Ejecuta la consulta y obtén los resultados
#resultados = collection.find(consulta)
resultados = collection.find()

# Imprime los resultados
print("encontrados:")
for producto in resultados:
    print(producto)


# Falta de que haga una consulta'''

















'''
#filename='./../data_Prim_ord/json/cligentes.json'
filename='./../../../../data_bda/json/clientes.json'
clients = read_json_file(filename)

clients_collection.insert_one({"clients": clients}) # Inserta la lista de clients


print("Contenido de la colección 'clients':")       # Imprimir
for clients in clients_collection.find():
    print(clients)'''
    
