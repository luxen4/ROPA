#Leer del csv y meter en Mysql
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




zapatillas=[]
def read_csv_file(filename):
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
        
            insertTable(style, marca, model, years, precio)


dropTable()
createTable()

file_name='./zapatillas.csv'
read_csv_file(file_name)