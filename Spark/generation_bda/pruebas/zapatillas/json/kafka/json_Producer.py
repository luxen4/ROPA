    # Mandar por kafka
from time import sleep                      
from json import dumps
import csv
from kafka import KafkaProducer



import json
# Leer un json
def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return None

if __name__ == "__main__":
       
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    # producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    file_name='./../zapatillas.json'
    data = read_json_file(file_name)
    
    if data is not None:
        for registro in data:
            style = registro['style']
            marca = registro['marca']
            model = registro['model']
            years = registro['years']
            
            message = { "style": style, "marca": marca,"model": model, "years": years }
            print(message)
            producer.send('zapatillas_stream', value=message)

    
#sleep(2)

   
    
    
    # Desde una consulta a neo4j hacer el Producer