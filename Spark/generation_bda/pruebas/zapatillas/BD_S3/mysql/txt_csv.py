from pymongo import MongoClient
import json, csv


  
# Guardar los resultados en un archivo CSV  
def create_csv_file(filename, data):
    
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file) 

        writer.writerow(["style", "marca", "model", "years", "precio"])  # Escribir el encabezado
        
        for product in data:
            writer.writerow([product['style'], product['marca'], product['model'], product['years'], product['precio']])
    
    print(f"Archivo CSV '{filename}' generado exitosamente.")



zapatillas=[]
def read_text_file(filename):
    try:
       
        with open(filename, 'r') as file:
            
            for line in file:
                line = line.strip()
                if len(line) > 0:  
                    style = line.split(',')[0]
                    marca = line.split(',')[1] 
                    model = line.split(',')[2]
                    years = line.split(',')[3]
                    precio= line.split(',')[4]
                    
                    zapatilla={"style":style, "marca":marca, "model":model, "years":years, "precio":precio}
                    
                    zapatillas.append(zapatilla)
                
            return zapatillas


    except FileNotFoundError:
        print(f"File '{filename}' not found.")


file_name="./zapatillas2.txt"
data = read_text_file(file_name)

file_name="./result/csv/zapatillas.csv"
create_csv_file(file_name, data)