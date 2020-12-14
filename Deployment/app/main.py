from datetime import datetime, timedelta

import extraction.extract_main as extract_main
from transform import transform_data
import sys, os
import pandas as pd

sys.path.append('../')


def create_folder (path):
    try:
        os.stat(path)
    except:
        os.makedirs(path)   


if __name__ == '__main__':
#def run(path_gcs_params):


    try:
        df_consoles = pd.read_csv('data/consoles.csv')
    except Exception as e:
        msg_error = 'Error leer consoles.csv => {}'
        print(msg_error.format(e))
        exit (0)


    try:
        df_result = pd.read_csv('data/result.csv')
    except Exception as e:
        msg_error = 'Error leer result.csv => {}'
        print(msg_error.format(e))
        exit (0)


    #Creamos la carpeta para la zona DWH
    path_dwh = 'data_dwh'
    create_folder(path_dwh)
    transform_data.generate_dm_console(df_consoles).to_csv(path_dwh+'/dm_console.csv', index=False)
    transform_data.generate_dm_game(df_result).to_csv(path_dwh+'/dm_game.csv',index=False)
    transform_data.generate_dm_company(df_consoles).to_csv(path_dwh+'/dm_company.csv',index=False)
    transform_data.generate_ft_critics(df_result, df_consoles).to_csv(path_dwh+'/ft_metrics.csv',index=False)
