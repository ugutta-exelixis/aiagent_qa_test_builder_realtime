import pandas as pd
import os

def convert_to_json(source, destination):
    df = pd.read_csv(source)
    df.to_json(destination,orient='records')
    print ('Done')


if __name__== '__main__':
    Source_directory = '303_Ingestion_Config.csv'
    Destination = '303_Ingestion_Config.json'
    convert_to_json(Source_directory, Destination)
