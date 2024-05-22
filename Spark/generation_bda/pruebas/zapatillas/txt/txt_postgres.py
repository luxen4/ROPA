# 1º Crear las tablas de cada uno
import psycopg2
import csv
  
  
def dropTable_zapatillas():
    try:
        connection = psycopg2.connect( host="localhost", port="5432", database="tienda_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        #connection = psycopg2.connect( host="localhost", port="9999", database="primOrd_db", user="primOrd", password="bdaPrimOrd")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()

        create_table_query = """ DROP TABLE IF EXISTS zapatillas; """
        
        
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'ZAPATILLAS' deleted successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)
  
  
def createTable_zapatillas():
    try:
        connection = psycopg2.connect( host="localhost", port="5432", database="tienda_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        #connection = psycopg2.connect( host="localhost", port="9999", database="primOrd_db", user="primOrd", password="bdaPrimOrd")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS zapatillas (
                zapatilla_id SERIAL PRIMARY KEY,
                style VARCHAR (100),
                marca VARCHAR (100),
                model VARCHAR (100),
                years VARCHAR (100),
                precio DECIMAL (10,2)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'ZAPATILLAS' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)

def insertar_Zapatillas(style,marca,model,years,precio):
    
    #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    connection = psycopg2.connect( host="localhost", port="5432", database="tienda_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO zapatillas (style,marca,model,years,precio) VALUES (%s, %s, %s, %s, %s);", 
                   (style,marca,model,years,precio))

    
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla ZAPATILLAS.")
     

def read_text_file(filename):
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        
        with open(filename, 'r') as file:
            
            for line in file:
                line = line.strip()
                if len(line) > 0: 
                    style = line.split(',')[0]
                    marca = line.split(',')[1] 
                    model = line.split(',')[2]
                    years = line.split(',')[3]
                    precio= line.split(',')[4]
                    
                    insertar_Zapatillas(style,marca,model,years,precio)
                    
                   

    except FileNotFoundError:
        print(f"File '{filename}' not found.")


dropTable_zapatillas()           
createTable_zapatillas()

filename = "./zapatillas2.txt"
read_text_file(filename)

