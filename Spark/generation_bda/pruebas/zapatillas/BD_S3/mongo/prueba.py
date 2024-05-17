import pymongo

# Conexión a MongoDB
client = pymongo.MongoClient('mongodb://root:secret@localhost:27017/')

# Seleccionar la base de datos y la colección
db = client["proyecto"]
zapatillas_collection = db["zapatillas"]

documento = {
    "style": "AAAAAAAAAAAAAAAAA",
    "marca": "Nike",
    "modelo": "Air Max",
    "año": 2022,
    "precio": 120
}

# Insertar el documento en la colección
try:
    resultado = zapatillas_collection.insert_one(documento)
    print("Documento insertado exitosamente.")
except Exception as e:
    print(f"Error al insertar documento: {e}")