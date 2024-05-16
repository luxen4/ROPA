    # Mandar por kafka
from time import sleep                      
from json import dumps
from kafka import KafkaProducer
from neo4j import GraphDatabase




class Neo4jClient:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None

    def connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def get_all_Zapatillas(self, session):
        result = session.run("MATCH (z:Zapatillas) RETURN z")
        return result



if __name__ == "__main__":
       
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    # producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    neo4j_client = Neo4jClient("bolt://localhost:7687", "neo4j", "your_password")               # Crear instancia de Neo4jClient
    neo4j_client.connect()
                                                                                                # Crear una sesión de Neo4j
    with neo4j_client._driver.session() as session:
        menus = neo4j_client.get_all_Zapatillas(session)                                           

        # Zapatillas
        for record in menus:
            print(record)
            zap = record['z']
            print(f"style: {zap['style']}, marca: {zap['marca']}, model: {zap['model']}, years: {zap['years']}, , precio: {zap['precio']}")
            message = { "style": zap['style'], "marca": zap['marca'],"model": zap['model'], "years": zap['years'], "precio":zap['precio'] }
            print(message)
            producer.send('zapatillas_stream', value=message)
            
        #sleep(2)

    neo4j_client.close() # Cerrar la conexión con Neo4j
    
    
    # Desde una consulta a neo4j hacer el Producer