import pandas as pd
import openpyxl

import datetime
import json

def run(clients, file, **args):
    s3_client, postgresql_engine = clients

    conn = postgresql_engine.connect()
    
    # Load sheet as dataframe
    fc = pd.read_excel(file, sheet_name=0, skiprows = 5, usecols="H, M:X")
    fc.dropna(inplace=True)

    print(fc)

    return True