from pymongo import MongoClient
import json

def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return None
    
class MongoDBOperations:
    def __init__(self, database_name, port,username=None, password=None):
        if username and password:
            self.client = MongoClient(f'mongodb://{username}:{password}@localhost:{{port}}/')
        else:
            self.client = MongoClient(f'mongodb://localhost:{port}/')
        self.db = self.client[database_name]
        
    def create_person(self, collection_name,data):
        self.collection = self.db[collection_name]
        result = self.collection.insert_one(data)
        return result
    
    
    
  
file_name='./Spark/generation_bda/pruebas/zapatillas/BD_S3/mongo/zapatillas.json'
empleados=read_json_file(file_name)


mongo_operations = MongoDBOperations('BDAExamen','27017')

for data in empleados:
    print(empleados)
    mongo_operations.create_person("empleado",data)
    
    


