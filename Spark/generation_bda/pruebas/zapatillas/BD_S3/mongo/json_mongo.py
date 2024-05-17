from pymongo import MongoClient
import json

# Usando localhost si has mapeado el puerto 27017 del contenedor al host local
#clients_collection = MongoClient('mongodb://root:secret@spark-mongodb-1:27017/')

clients_collection = MongoClient()


# Selecciona la base de datos y la colección
db = clients_collection["proyecto"]
zapatillas_collection = db["zapatillas"]


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

print(data)

#zapatillas_collection = db.create_collection("zapatillas")

# Verifica si el documento JSON se leyó correctamente
if data:
    # Inserta el documento en la colección zapatillas
    result = zapatillas_collection.insert_one({"zapatillas": data})
    print("Zapatillas insertadas exitosamente.")

# db.zapatillas.insertOne({"style": "Casual", "marca": "Nike", "model": "Air Max","years": "2022","precio": "120"});
# db.zapatillas.insertOne({"style": "Casual"});

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
    
