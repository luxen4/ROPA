import json, csv

# Guardar los resultados en un archivo CSV  
def create_csv_file(filename, data):
    
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file) 

        writer.writerow(["style", "marca", "model", "years", "precio"])  # Escribir el encabezado
        
        for product in data:
            writer.writerow([product['style'], product['marca'], product['model'], product['years'], product['precio']])
    
    print(f"Archivo CSV '{filename}' generado exitosamente.")
    
    

# Leer un json
def read_json_file(filename):
    
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError as e:
        print(e)
        return None


file_name='./Spark/generation_bda/pruebas/zapatillas/json/zapatillas.json'
data = read_json_file(file_name)


file_name='./Spark/generation_bda/pruebas/zapatillas/json/zapatillasADRIAN.csv'
create_csv_file(file_name, data)

