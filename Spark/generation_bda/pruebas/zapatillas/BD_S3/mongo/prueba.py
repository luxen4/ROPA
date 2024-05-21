from pymongo import MongoClient

# Conexión a MongoDB
clients_collection = MongoClient('mongodb://localhost:27017/?authSource=proyecto')

# Selecciona la base de datos y la colección
db = clients_collection["proyecto"]
zapatillas_collection = db["zapatillas"]

# Ahora puedes realizar operaciones en la colección zapatillas
# Por ejemplo, insertar un documento
try:
    zapatillas_collection.insert_one({"style": "Casual", "marca": "Nike", "modelo": "Air Max", "año": 2022, "precio": 120})
    print("Documento insertado exitosamente.")
except Exception as e:
    print(f"Error al insertar documento: {e}")