import mysql.connector
import random, string


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

        sql = """
        CREATE TABLE IF NOT EXISTS zapatillas(
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



def read_text_file(filename):
    try:
        conexion = mysql.connector.connect( host="localhost", user="root", password="alberite", database="tienda_db")
        cursor = conexion.cursor()
        
        with open(filename, 'r') as file:
            
            for line in file:
                line = line.strip()
                if len(line) > 0:  
                    style = line.split(',')[0]
                    marca = line.split(',')[1] 
                    model = line.split(',')[2]
                    years = line.split(',')[3]
                    precio= line.split(',')[4]
                
                    cursor = conexion.cursor()                                      # Crear un cursor para ejecutar consultas
                    
                    cursor.execute(""" INSERT INTO zapatillas (style, marca, model, years, precio) VALUES (%s, %s, %s, %s, %s) """, 
                                (style, marca, model, years, precio))
                    
                    conexion.commit()                                               # Hacer commit para confirmar la transacción
                    print("Registro insertado")
            
            cursor.close()                                                          # Cerrar el cursor y la conexión
            conexion.close()

    except FileNotFoundError:
        print(f"File '{filename}' not found.")

dropTable()
createTable()

filename = "./zapatillas2.txt"
read_text_file(filename)


    # email VARCHAR(255) NOT NULL,
    # bird_date DATE
    # created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP