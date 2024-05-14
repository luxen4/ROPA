from pymongo import MongoClient
import json, csv


  
# Guardar los resultados en un archivo CSV  
def create_csv_file(filename, data):
    
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file) 

        writer.writerow(["style", "marca", "model", "years"])  # Escribir el encabezado
        
        for product in data:
            writer.writerow([product['style'], product['marca'], product['model'], product['years']])
    
    print(f"Archivo CSV '{filename}' generado exitosamente.")



file_name="./zapatillas2.txt"
create_csv_file(file_name)