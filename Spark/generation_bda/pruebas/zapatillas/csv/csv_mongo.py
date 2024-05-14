import csv
from pymongo import MongoClient

client = MongoClient()                  # Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)

db = client["proyecto"]
clients_collection = db["zapatillas2"]      # Accede a la colección "clients"


zapatillas=[]
def read_csv_file(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            print(row)
            style = row[0]
            marca = row[1]
            model = row[2]
            years = row[3]
            precio = row[4]
            
            zapatilla = {"style":style, "marca":marca, "model": model, "years":years, "precio":precio}
            zapatillas.append(zapatilla)
        
            clients_collection.insert_one({"zapatillas2": zapatillas}) # Inserta la lista de clients
            print("Zapatillas insertados con exito.")


file_name='./zapatillas.csv'
read_csv_file(file_name)