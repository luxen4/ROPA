from neo4j import GraphDatabase
import json, csv

# Crear los registros
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

    def create_Relog(self, session, marca, serie, modelo, precio):
        result = session.run("CREATE (r:Reloges {marca: $marca, serie: $serie, modelo: $modelo, precio: $precio}) RETURN r", 
                              marca=marca, serie=serie, modelo=modelo, precio=precio)
    
        return result.single()[0]


# Crear instancia de Neo4jClient
neo4j_client = Neo4jClient("bolt://localhost:7687", "neo4j", "your_password")
neo4j_client.connect()


                                                                        # Leer el archivo .txt
def read_text_file(filename):
    with neo4j_client._driver.session() as session:                     # Crea la sesión

        try:
            with open(filename, 'r') as file: 
                
                    for line in file:
                        line = line.strip()
                        if len(line) > 0:  
                            print(line)    
                            marca = line.split(':')[0]
                            serie = line.split(':')[1]
                            modelo = line.split(':')[2]
                            precio = line.split(':')[3]
                            
                        zapa = neo4j_client.create_Relog(session,  marca, serie, modelo, precio)
                        print(f"Relog Creado -> marca: {marca},serie:{serie} modelo: {modelo}, precio: {precio}")
                
        except FileNotFoundError:
            print(f"File '{filename}' not found.")
            
        
neo4j_client.close()

file_name="./relojes1.txt"
read_text_file(file_name)