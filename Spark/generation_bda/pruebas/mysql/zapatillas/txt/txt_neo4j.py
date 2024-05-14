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



# Necesito leer un archivo menus.csv
# Function to read and display the content of a CSV file
import csv, json

menus=[]
def read_csv_fileMenus(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            menu ={"id_menu": row[0], "precio": row[1], "disponibilidad": row[2], "id_restaurante": row[3]}
            menus.append(menu)
        return menus


 
    
    
def read_text_file(filename):
    try:
        with neo4j_client._driver.session() as session:
            with open(filename, 'r') as file:
                
                for line in file:
                    line = line.strip()
                    if len(line) > 0:  
                        style = line.split(',')[0]
                        marca = line.split(',')[1] 
                        model = line.split(',')[2]
                        years = line.split(',')[3]
                        precio= line.split(',')[4]
                    
                        nodo = neo4j_client.create_zapatilla(session,  style, marca, model, years, precio)
                        print(f"style: {style}, marca: {marca}, model: {model}, years: {years}, precio:{precio}")

                        print("Zapatilla insertada")
            

    except FileNotFoundError:
        print(f"File '{filename}' not found.")


filename='./zapatillas2.txt'
read_text_file(filename)   
    
    
    
    
    
    
    
    
    
    
    
    


# Inserta desde un json en neo4j
filename = "./../../../data_bda/json/relaciones.json"
relaciones = read_json_fileRelaciones(filename)

with neo4j_client._driver.session() as session:
    for relacion in relaciones:
        id_menu = relacion['id_menu']
        id_restaurante = relacion['id_plato']
        
        relac = neo4j_client.create_relacion(session,  id_menu, id_restaurante)
        
        print(f"Relación creada -> id_menu: {relacion['id_menu']}, id_plato: {relacion['id_plato']}")
neo4j_client.close()
#___ 