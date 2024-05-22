
import mysql.connector
from time import sleep                      
from json import dumps
from kafka import KafkaProducer



zapatillas=[]
# Leer el archivo .txt
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
                    
                    item = { "style": style, "marca": marca,"model": model, "years": years, "precio":precio }
                    zapatillas.append(item)
                    
            
            return zapatillas
            
    except FileNotFoundError:
        print(f"File '{filename}' not found.")


if __name__ == "__main__":
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    # producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    file_name="Spark/data_bda/txt/zapatillas.txt"
    resultados = read_text_file(file_name)
    
    for item in resultados:
        style=item['style']
        marca=item['marca']
        model=item['model']
        years=item['years']
        precio=item['precio']
        
        message = { "style": style, "marca": marca,"model": model, "years": years, "precio": precio }
        print(message)
        producer.send('zapatillas_stream', value=message)
        
        sleep()
        
# que lea de txt y que haga el producer