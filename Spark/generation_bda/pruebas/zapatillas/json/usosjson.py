import json

# Leer un json, recibe la ruta del archivo
def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            print(data)
            return data
    except FileNotFoundError:
        return None