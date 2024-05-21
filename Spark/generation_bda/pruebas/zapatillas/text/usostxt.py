

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

    except FileNotFoundError:
        print(f"File '{filename}' not found.")