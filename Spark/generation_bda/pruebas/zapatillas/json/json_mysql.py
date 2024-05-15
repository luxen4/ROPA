import mysql.connector
import csv

def dropTable():
    try:
        conexion = mysql.connector.connect( host="localhost",user="root",password="alberite",database="tienda_db")
        cursor = conexion.cursor()

        sql = """ DROP TABLE IF EXISTS zapatillas; """

        cursor.execute(sql)
        conexion.commit()
        cursor.close()
        conexion.close()

        print("Table 'ZAPATILLAS' eliminada exitosamente.")
        
    except Exception:
        #print(f"File '{filename}' not found.")
        print("No se ha podido eliminar la tabla.")

def createTable():
    try:
        conexion = mysql.connector.connect( host="localhost",user="root",password="alberite",database="tienda_db")
        cursor = conexion.cursor()

        sql = """Create table zapatillas(
            zapatilla_id INT AUTO_INCREMENT PRIMARY KEY,
            style VARCHAR(100),
            marca VARCHAR(100),
            model VARCHAR(100),
            years VARCHAR(100),
            precio DECIMAL (10.2)
        );
        """

        cursor.execute(sql)
        conexion.commit()
        cursor.close()
        conexion.close()

        print("Table 'ZAPATILLAS' creada exitosamente.")
        
    except Exception:
        #print(f"File '{filename}' not found.")
        print("No se ha podido crear la tabla.")



def insertTable(style, marca, model, years, precio):
    try:
        conexion = mysql.connector.connect( host="localhost",user="root",password="alberite",database="tienda_db")
        cursor = conexion.cursor()

        cursor.execute(""" INSERT INTO zapatillas (style, marca, model, years, precio) VALUES (%s, %s, %s, %s, %s) """, 
                                (style, marca, model, years, precio))

        conexion.commit()
        cursor.close()
        conexion.close()

        print("Registro insertado.")
        
    except Exception:
        #print(f"File '{filename}' not found.")
        print("No se ha podido insertar en la tabla.")




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
    
file_name='./zapatillas.json'
data = read_json_file(file_name)
    
for registro in data:
        style = registro['style']
        marca = registro['marca']
        model = registro['model']
        years = registro['years']
        precio = registro['precio']
            
        insertTable(style, marca, model, years, precio)
        print(f"style: {style}, marca: {marca}, model: {model}, years: {years}, precio:{precio}")