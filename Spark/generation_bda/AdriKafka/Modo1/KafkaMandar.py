from confluent_kafka import Producer
import json
import random
import time

def generate_random_data():
    timestamp = int(time.time() * 1000)  # Genera un timestamp UNIX en milisegundos
    store_id = random.randint(1, 100)    # Genera un ID de tienda aleatorio entre 1 y 100
    product_id = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))  # Genera un ID de producto alfanumérico aleatorio
    quantity_sold = random.randint(1, 100)  # Genera una cantidad vendida aleatoria entre 1 y 100
    revenue = round(random.uniform(1.0, 1000.0), 2)  # Genera un ingreso aleatorio entre 1.0 y 1000.0

    # Crea un diccionario con los atributos del mensaje
    message = {
        "timestamp": timestamp,
        "store_id": store_id,
        "product_id": product_id,
        "quantity_sold": quantity_sold,
        "revenue": revenue
    }

    return message

def send_to_kafka(message):
    # Configura los parámetros de conexión
    conf = {
        'bootstrap.servers': 'localhost:9092',  # Dirección del broker de Kafka
    }

    # Crea un productor Kafka
    producer = Producer(**conf)
    
    

    # Callback para manejar la entrega de mensajes
    def delivery_callback(err, msg):
        if err:
            print(f'Error al enviar mensaje a Kafka: {err}')
        else:
            print(f'Mensaje enviado a Kafka: {msg}')

    # Serializa el mensaje como JSON
    json_message = json.dumps(message)

    # Envía el mensaje a Kafka
    producer.produce('sales_stream', json_message.encode('utf-8'), callback=delivery_callback)

    # Espera a que los mensajes se entreguen o se produzca un error
    producer.flush()

    # Cierra el productor
    #producer.close()

if __name__ == '__main__':
    while True:
        # Genera datos aleatorios
        data = generate_random_data()
        
        # Envía los datos a Kafka
        send_to_kafka(data)

        # Espera un intervalo de tiempo antes de enviar el próximo mensaje
        time.sleep(1)  # Puedes ajustar este valor según sea necesario
        