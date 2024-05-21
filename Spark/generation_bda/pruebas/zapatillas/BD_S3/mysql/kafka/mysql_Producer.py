
import mysql.connector
from time import sleep                      
from json import dumps
from kafka import KafkaProducer


def selectTable():
    try:
        conexion = mysql.connector.connect( host="localhost",user="root",password="alberite",database="tienda_db")
        cursor = conexion.cursor()

        sql = """Select * from zapatillas; """

        cursor.execute(sql)
         # Obtener los resultados
        resultados = cursor.fetchall()
        
        print("Datos OK.")
        return resultados
        
    except Exception:
        #print(f"File '{filename}' not found.")
        print("No se ha podido crear la tabla.")


if __name__ == "__main__":
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    # producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

    resultados = selectTable()
    print(resultados)
    data=[]
    for item in resultados:
        print(item)  # Aquí puedes hacer algo con cada fila
        style=item[0]
        marca=item[1]
        model=item[2]
        years=item[3]
        
        message = { "style": style, "marca": marca,"model": model, "years": years }
        print(message)
        producer.send('zapatillas_stream', value=message)
        
        #sleep(2)
        
# que lea de mysql y que haga el producer