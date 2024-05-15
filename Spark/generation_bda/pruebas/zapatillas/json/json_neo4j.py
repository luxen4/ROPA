import csv
from neo4j import GraphDatabase

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

    def create_zapatilla(self, session, style, marca, model, years, precio):
        result = session.run("CREATE (z:Zapatillas {style:$style, marca: $marca, model: $model, years: $years, precio:$precio}) RETURN z", 
                                style=style, marca=marca, model=model, years=years, precio=precio)
    
        return result.single()[0]



# Crear instancia de Neo4jClient
neo4j_client = Neo4jClient("bolt://localhost:7687", "neo4j", "your_password")
neo4j_client.connect()



import json
# Leer un json
def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            print(data)
            return data
    except FileNotFoundError:
        return None



zapatillas=[]
def read_csv_file(filename):
    with neo4j_client._driver.session() as session:
    
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
            
                nodo = neo4j_client.create_zapatilla(session,  style, marca, model, years, precio)
                print(f"style: {style}, marca: {marca}, model: {model}, years: {years}, precio:{precio}")

                print("Zapatilla insertada")           
            
            
file_name='./zapatillas.json'
data = read_json_file(file_name)


with neo4j_client._driver.session() as session:
    for registro in data:
        style = registro['style']
        marca = registro['marca']
        model = registro['model']
        years = registro['years']
        precio = registro['precio']
            
        nodo = neo4j_client.create_zapatilla(session,  style, marca, model, years, precio)
        print(f"style: {style}, marca: {marca}, model: {model}, years: {years}, precio:{precio}")

        print("Zapatilla insertada") 
