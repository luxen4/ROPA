import csv, os, random

# Function to create a new CSV file with sample data
def create_csv_file(filename):
    filas=[]

    for i in range(1, 2001):
        dia = random.randint(1, 31)
        mes = random.randint(1, 13)
        ano = random.randint(2000, 2024)
        
        # Probabilidad del 5% de que la fecha sea nula o tenga un formato incorrecto
        probabilidad = random.random()
        if probabilidad < 0.05:
            date = None
        else:
            date = f"{dia}-{mes}-{ano}"
        
        store_ID = random.randint(1, 8)
        product_ID = random.randint(1, 10)
        quantity_sold = random.randint(1, 101)
        revenue = random.randint(50, 5001)

        
        # Probabilidad del 10% de que el product_ID sea nulo o vacío
        probabilidad = random.random()
        if probabilidad < 0.05:
            quantity_sold = None
        # Probabilidad del 5% de que el product_ID sea un valor no válido
        elif probabilidad < 0.1:
            quantity_sold = 0
            
            
        # revenue 
        probabilidad = random.random()
        if probabilidad < 0.05:   revenue = None
        elif probabilidad < 0.1:  revenue = 0
        elif probabilidad < 0.15: revenue = "az"

        fila = [date, store_ID, product_ID, quantity_sold, revenue]
        filas.append(fila) 
    
    # Añadir encabezados
    data = [["date", "store_ID","product_ID","quantity_sold","revenue"]] + filas

    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

    print(f"Created {filename}")

create_csv_file("sales_dataa.csv")

# Este archivo se ejecuta desde 
# root@03c14ab5a86c:/opt/spark-apps/AdriFabricarCSV# python prueba.py