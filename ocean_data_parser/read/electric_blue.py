import pandas as pd
import re

def csv(path,encoding='utf-8',):
    with open(path,encoding=encoding) as f:
        line = True
        metadata = {}
        while line:
            line = f.readline()
            line = line.strip()
            if re.match(r'^[-,]+$',line):
                continue
            elif line.startswith('time,'):
                break
            else:
                items = line.split(', ', 1)
                key = items[0]
                value = items[1] if len(items)==2 else ""

                attr = re.sub(r'[\s\[\]\(\)]+','_',key.lower())

                #cast value
                if re.match(r'^[+-]\d+$',value):
                    value = int(value)
                elif re.match('^[0-9]\d+\.\d+$',value):
                    value = float(value)
 
                metadata[attr] = value
        
        columns = line.split(',')
        df = pd.read_csv(f, sep=',',header=None, names=columns, parse_dates=True,index_col=['time'])

        ds = df.to_xarray()
        ds.attrs = metadata
        return ds

def log_csv(path,encoding="UTF-8"):

    df = pd.read_csv(path, encoding=encoding,parse_dates=True,index_col=['time'])
    ds = df.to_xarray()
    # add default attributes
    return ds
        
                
        