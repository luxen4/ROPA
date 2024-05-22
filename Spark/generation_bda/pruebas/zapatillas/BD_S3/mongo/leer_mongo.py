from pymongo import MongoClient
from time import sleep                      
from json import dumps
from kafka import KafkaProducer


if __name__ == "__main__":
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    # producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

    client = MongoClient(f'mongodb://localhost:27017/')
    db = client["BDAExamen"]
    collection = db["empleado"]       # Accede a la colección
    resultados = collection.find()

    print("encontrados:")
    for item in resultados:
        print(item)
        style= item['style']
        marca= item['marca']
        model= item['model']
        message = { "style": style, "marca": marca,"model": model}
        print(message)
        producer.send('zapatillas_stream', value=message)
        
        #sleep(2)
