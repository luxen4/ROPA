from pymongo import MongoClient
import json, csv

# Function to create a JSON file
def create_json_file(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file)
        file.write('\n')
    print(f"File '{filename}' created successfully!")
  
  
# Guardar los resultados en un archivo CSV  
def create_csv_file(filename, data):
    
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file) 

        writer.writerow(["style", "marca", "model", "years"])  # Escribir el encabezado
        
        for product in data:
            writer.writerow([product['style'], product['marca'], product['model'], product['years']])
    
    print(f"Archivo CSV '{filename}' generado exitosamente.")



zapatillas=[]
# Leer el archivo .txt
def read_text_file(filename):
    try:
        with open(filename, 'r') as file: 
            # content = file.read()
            # print(f"Contents of '{filename}':\n{content}") 
               
               
                for line in file:
                    line = line.strip()
                    if len(line) > 0:  
                        print(line)       
                        style = line.split(',')[0]
                        marca = line.split(',')[1]
                        model = line.split(',')[2]
                        years = line.split(',')[3]
                    zapatilla={"style":style, "marca":marca, "model":model, "years":years}
                    zapatillas.append(zapatilla)
                
                create_json_file("./zapatillas.json",zapatillas)
                create_csv_file("./zapatillas.csv", zapatillas)
            
    except FileNotFoundError:
        print(f"File '{filename}' not found.")

file_name="./zapatillas2.txt"
read_text_file(file_name)