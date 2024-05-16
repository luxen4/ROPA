    # Mandar por kafka
from time import sleep                      
from json import dumps
import csv
from kafka import KafkaProducer

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
            
            message = { "style": style, "marca": marca,"model": model, "years": years, "precio":precio }
            print(message)
            producer.send('zapatillas_stream', value=message)

if __name__ == "__main__":
       
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    # producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    file_name='./../zapatillas.csv'
    read_csv_file(file_name)
    
#sleep(2)

   
    
    
    # Desde una consulta a neo4j hacer el Producer