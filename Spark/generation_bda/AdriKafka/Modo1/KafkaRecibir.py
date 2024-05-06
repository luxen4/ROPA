from confluent_kafka import Consumer, KafkaError
import signal

def consume_messages():
    # Configura los parámetros de conexión
    conf = {
        'bootstrap.servers': 'localhost:9092',  # Direcciones de los brokers de Kafka
        'group.id': 'my_consumer_group',        # Identificador del grupo de consumidores
        'auto.offset.reset': 'earliest'         # Configuración de inicio desde el principio del tema
    }

    # Crea un consumidor Kafka
    consumer = Consumer(conf)

    # Suscríbete a un tema
    consumer.subscribe(['sales_stream'])

    # Función para manejar la señal SIGINT (Ctrl + C)
    def stop_process(signal, frame):
        print('Deteniendo el proceso...')
        consumer.close()
        exit(0)

    # Asigna la función de manejo de señales a SIGINT
    signal.signal(signal.SIGINT, stop_process)

    try:
        # Loop de consumo de mensajes
        while True:
            msg = consumer.poll(timeout=1.0)  # Espera mensajes durante 1 segundo
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Final de partición, continue con el siguiente mensaje
                    continue
                else:
                    # Error de Kafka, manejarlo según tu lógica de aplicación
                    print("Error al consumir mensaje: {}".format(msg.error()))
                    break

            # Procesa el mensaje
            
            print('Mensaje recibido: {}' .format(msg.value().decode('utf-8')))

    except Exception as e:
        # Manejo de otras excepciones
        print("Error:", e)

    finally:
        # Cierra el consumidor
        consumer.close()

if __name__ == '__main__':
    consume_messages()
    