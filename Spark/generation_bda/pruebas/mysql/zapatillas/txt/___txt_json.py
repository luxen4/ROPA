import json

# Function to create a JSON file
def create_json_file(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file)
        file.write('\n')
    print(f"File '{filename}' created successfully!")
  

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
            
    except FileNotFoundError:
        print(f"File '{filename}' not found.")

file_name="./zapatillas2.txt"
read_text_file(file_name)