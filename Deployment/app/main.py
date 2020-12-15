from app.extraction import extract_data
from app.transform import transform_data
from app.load import load_data


from datetime import datetime, timedelta

import os
import pandas as pd


def create_folder (path):
    if not os.path.exists(path):
      os.makedirs(path)


#if __name__ == '__main__':
def run():



    #01 Extraction data from storage
    path_raw = 'data_raw'
    create_folder(path_raw)
    try:
        extract_data.download_data_csv('data_raw_metacritic_games','result.csv', 'data_raw/result.csv')

        extract_data.download_data_csv('data_raw_metacritic_games','consoles.csv', 'data_raw/consoles.csv')
    except Exception as e:
            msg_error = 'Error al descargar los origenes result.csv o consoles.csv => {}'
            print(msg_error.format(e))
            exit (0)



    try:
        df_consoles = pd.read_csv('{}/consoles.csv'.format(path_raw))
    except Exception as e:
        msg_error = 'Error leer consoles.csv => {}'
        print(msg_error.format(e))
        exit (0)


    try:
        df_result = pd.read_csv('{}/result.csv'.format(path_raw))
    except Exception as e:
        msg_error = 'Error leer result.csv => {}'
        print(msg_error.format(e))
        exit (0)



    #Creamos la carpeta para la zona DWH
    path_dwh = 'data_dwh'
    create_folder(path_dwh)
    try:
        transform_data.generate_dm_console(df_consoles).to_csv(path_dwh+'/dm_console.csv', index=False)
        transform_data.generate_dm_game(df_result).to_csv(path_dwh+'/dm_game.csv',index=False)
        transform_data.generate_dm_company(df_consoles).to_csv(path_dwh+'/dm_company.csv',index=False)
        transform_data.generate_ft_critics(df_result, df_consoles).to_csv(path_dwh+'/ft_critics.csv',index=False)
    except Exception as e:
        msg_error = 'Error en la zona de transformacion dwh => {}'
        print(msg_error.format(e))
        exit (0)

    #Load data dwh to storage

    try:
        data_dwh_metacritic_games = 'data_dwh_metacritic_games'
        load_data.upload_blob(data_dwh_metacritic_games,'data_dwh','dm_console.csv')
        load_data.upload_blob(data_dwh_metacritic_games,'data_dwh','dm_game.csv')
        load_data.upload_blob(data_dwh_metacritic_games,'data_dwh','dm_company.csv')
        load_data.upload_blob(data_dwh_metacritic_games,'data_dwh','ft_critics.csv')
    except Exception as e:
        msg_error = 'Error en subida de archivos dwh a storage=> {}'
        print(msg_error.format(e))
        exit (0)


    #Load data dwh to BigQuery
    csv_delimiter = ','
    dataset_bq_dwh = 'data_dwh_metacritic_games'
    try:
        load_data.load_csv_into_bq(data_dwh_metacritic_games, 'dm_console.csv', csv_delimiter, dataset_bq_dwh, 'dm_console')
        load_data.load_csv_into_bq(data_dwh_metacritic_games, 'dm_game.csv', csv_delimiter, dataset_bq_dwh, 'dm_game')
        load_data.load_csv_into_bq(data_dwh_metacritic_games, 'dm_company.csv', csv_delimiter, dataset_bq_dwh, 'dm_company')
        load_data.load_csv_into_bq(data_dwh_metacritic_games, 'ft_critics.csv', csv_delimiter, dataset_bq_dwh, 'ft_critics')

    except Exception as e:
        msg_error = 'Error en carga de tablas en dataset dwh => {}'
        print(msg_error.format(e))
        exit (0)

    #Se aplican las querys a la zona dwh para entregar los reportes que necesita el area de analytics

    dataset_bq_dwh = 'data_analytics_metacritic_games'
    sql_bestgames_console_company = '''
        WITH temp AS (
        SELECT 
              com.name AS company
             ,con.name AS console
             ,gam.name AS game
             ,ROW_NUMBER() OVER(PARTITION BY con.name, com.name  ORDER BY  CASE WHEN SAFE_CAST(metascore AS INT64) IS NULL THEN -1 ELSE CAST(metascore AS INT64) END desc) as ranking_metascore
             ,ROW_NUMBER() OVER(PARTITION BY con.name, com.name  ORDER BY  CASE WHEN SAFE_CAST(userscore AS INT64) IS NULL THEN -1 ELSE CAST(userscore AS INT64) END desc) as ranking_userscore
             ,ROW_NUMBER() OVER(PARTITION BY con.name, com.name 
                              ORDER BY  CASE WHEN SAFE_CAST(metascore AS INT64) IS NULL THEN -1 ELSE CAST(metascore AS INT64) END desc
                                       ,CASE WHEN SAFE_CAST(userscore AS INT64) IS NULL THEN -1 ELSE CAST(userscore AS INT64) END desc) as rank
              FROM `de-wallmart.data_dwh_metacritic_games.ft_critics` ft
        INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_console` con
                ON ft.id_console = con.id_console
        INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_company` com
                ON ft.id_company = com.id_company
        INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_game` gam
                ON ft.id_game = gam.id_game  
        ) SELECT 
                * EXCEPT(ranking_metascore,ranking_userscore)
                FROM temp
                where rank<=10
    '''
    load_data.execute_sql_bq(dataset_bq_dwh, 'bestgames_console_company', sql_bestgames_console_company)


    sql_bestgames_worst_company = '''
        WITH temp AS (
        SELECT 
              com.name AS company
             ,con.name AS console
             ,gam.name AS game
             ,ft.date  AS date
             ,current_timestamp() as load_timestmap
             ,ROW_NUMBER() OVER(PARTITION BY con.name, com.name  ORDER BY  CASE WHEN SAFE_CAST(metascore AS INT64) IS NULL THEN 100 ELSE CAST(metascore AS INT64) END asc) as ranking_metascore
             ,ROW_NUMBER() OVER(PARTITION BY con.name, com.name  ORDER BY  CASE WHEN SAFE_CAST(userscore AS INT64) IS NULL THEN 100 ELSE CAST(userscore AS INT64) END asc) as ranking_userscore
             ,ROW_NUMBER() OVER(PARTITION BY con.name, com.name 
                              ORDER BY  CASE WHEN SAFE_CAST(metascore AS INT64) IS NULL THEN 100 ELSE CAST(metascore AS INT64) END asc
                                       ,CASE WHEN SAFE_CAST(userscore AS INT64) IS NULL THEN 100 ELSE CAST(userscore AS INT64) END asc) as rank
              FROM `de-wallmart.data_dwh_metacritic_games.ft_critics` ft
        INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_console` con
                ON ft.id_console = con.id_console
        INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_company` com
                ON ft.id_company = com.id_company
        INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_game` gam
                ON ft.id_game = gam.id_game  
        ) SELECT 
                * EXCEPT(ranking_metascore,ranking_userscore)
                FROM temp
                where rank<=10
    '''
    load_data.execute_sql_bq(dataset_bq_dwh, 'worstgames_console_company', sql_bestgames_worst_company)





    sql_worst_all_consoles = '''
            WITH temp AS (
            SELECT 
                  com.name AS company
                 ,con.name AS console
                 ,gam.name AS game
                 ,ft.date  AS date
                 ,current_timestamp() as load_timestmap
                 ,ROW_NUMBER() OVER(PARTITION BY con.name ORDER BY  CASE WHEN SAFE_CAST(metascore AS INT64) IS NULL THEN 100 ELSE CAST(metascore AS INT64) END asc) as ranking_metascore
                 ,ROW_NUMBER() OVER(PARTITION BY con.name ORDER BY  CASE WHEN SAFE_CAST(userscore AS INT64) IS NULL THEN 100 ELSE CAST(userscore AS INT64) END asc) as ranking_userscore
                 ,ROW_NUMBER() OVER(PARTITION BY con.name 
                                  ORDER BY  CASE WHEN SAFE_CAST(metascore AS INT64) IS NULL THEN 100 ELSE CAST(metascore AS INT64) END asc
                                           ,CASE WHEN SAFE_CAST(userscore AS INT64) IS NULL THEN 100 ELSE CAST(userscore AS INT64) END asc) as rank
                  FROM `de-wallmart.data_dwh_metacritic_games.ft_critics` ft
            INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_console` con
                    ON ft.id_console = con.id_console
            INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_company` com
                    ON ft.id_company = com.id_company
            INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_game` gam
                    ON ft.id_game = gam.id_game  
            ) SELECT 
                    * EXCEPT(ranking_metascore,ranking_userscore)
                    FROM temp
                    where rank<=10
    '''
    load_data.execute_sql_bq(dataset_bq_dwh, 'worstgames_all_consoles', sql_worst_all_consoles)


    

    sql_best_all_consoles = '''
        WITH temp AS (
        SELECT 
              com.name AS company
             ,con.name AS console
             ,gam.name AS game
             ,ft.date  AS date
             ,current_timestamp() as load_timestmap
             ,ROW_NUMBER() OVER(PARTITION BY con.name ORDER BY  CASE WHEN SAFE_CAST(metascore AS INT64) IS NULL THEN -1 ELSE CAST(metascore AS INT64) END desc) as ranking_metascore
             ,ROW_NUMBER() OVER(PARTITION BY con.name ORDER BY  CASE WHEN SAFE_CAST(userscore AS INT64) IS NULL THEN -1 ELSE CAST(userscore AS INT64) END desc) as ranking_userscore
             ,ROW_NUMBER() OVER(PARTITION BY con.name 
                              ORDER BY  CASE WHEN SAFE_CAST(metascore AS INT64) IS NULL THEN -1 ELSE CAST(metascore AS INT64) END asc
                                       ,CASE WHEN SAFE_CAST(userscore AS INT64) IS NULL THEN -1 ELSE CAST(userscore AS INT64) END asc) as rank
              FROM `de-wallmart.data_dwh_metacritic_games.ft_critics` ft
        INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_console` con
                ON ft.id_console = con.id_console
        INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_company` com
                ON ft.id_company = com.id_company
        INNER JOIN `de-wallmart.data_dwh_metacritic_games.dm_game` gam
                ON ft.id_game = gam.id_game  
        ) SELECT 
                * EXCEPT(ranking_metascore,ranking_userscore)
                FROM temp
                where rank<=10
    

    '''
    load_data.execute_sql_bq(dataset_bq_dwh, 'bestgames_all_consoles', sql_best_all_consoles)
