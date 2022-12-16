import yaml 

def read_file(filepath): 
    try: 
        file=open(filepath, "r")
        ymlData=yaml.load(file, Loader=yaml.FullLoader)
        return ymlData 
    except Exception as ex: 
        raise ValueError(f'Not able to parse yaml Properly..{ex}')