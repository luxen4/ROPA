
import psycopg2
from time import sleep                      
from json import dumps
from kafka import KafkaProducer

def select_zapatillas():
    try:
        connection = psycopg2.connect( host="localhost", port="9999", database="tienda_db", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
        #connection = psycopg2.connect( host="localhost", port="9999", database="primOrd_db", user="primOrd", password="bdaPrimOrd")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()

        create_table_query = """ select * from zapatillas; """
        cursor.execute(create_table_query)
        
        
            # Obtener los resultados
        results = cursor.fetchall()
        return results
    
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)

if __name__ == "__main__":
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    # producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

    resultados = select_zapatillas()
    print(resultados)
    for row in resultados:
        style=row[0]
        marca=row[1]
        model=row[2]
        years=row[3]
        precio=row[4]
        
        item={"style":style,"marca":marca,"model":model,"years":years,"precio":precio}
            
        message = { "style": style, "marca": marca,"model": model, "years": years }
        print(message)
        producer.send('zapatillas_stream', value=message)
        
        #sleep(2)


       
# que lea de postgres y que haga el producer