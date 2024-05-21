from pymongo import MongoClient
from time import sleep                      
from json import dumps
from kafka import KafkaProducer


if __name__ == "__main__":
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    # producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

    client = MongoClient()
    db = client["proyecto"]
    collection = db["zapatillas1"]       # Accede a la colección
    resultados = collection.find()

    print("encontrados:")
    for item in resultados:
        zapa = item['zapatillas1']
        for zap in zapa:
            style= zap['style']
            marca= zap['marca']
            model= zap['model']
            years= zap['years']
            
            message = { "style": style, "marca": marca,"model": model, "years": years }
            print(message)
            producer.send('zapatillas_stream', value=message)
        
        #sleep(2)




# Pasos: 1-que lea de Mongo, 2-que haga el Producer