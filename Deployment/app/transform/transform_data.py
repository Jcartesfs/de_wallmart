import hashlib 
import datetime
import pandas as pd

def hash_md5(text):
    result = hashlib.md5(str(text).encode())
    result = result.hexdigest()
    return result

def generate_dm_console(df):
    columns = ['id_console','name','description','process_timestamp','start_timestamp','end_timestamp','creation_source_value','creation_source_desc','update_timestamp']
    df_aux = pd.DataFrame(columns = columns)
    df_aux['id_console'] = df['console'].apply(hash_md5)
    df_aux['name'] = df['console']
    df_aux['process_timestmap']  = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S"))
    df_aux['start_timestmap'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S"))
    df_aux['end_timestamp'] = None
    df_aux['creation_source_value'] = 'consoles.csv'
    df_aux['creation_source_desc'] = 'Archivo CSV'
    df_aux['update_timestamp'] = None
    return df_aux

def generate_dm_company(df):
    columns = ['id_company','name','description','process_timestamp','start_timestamp','end_timestamp','creation_source_value','creation_source_desc','update_timestamp']
    df = pd.DataFrame(df['company'].unique(),columns=['company'])
    df_aux = pd.DataFrame(columns = columns)
    df_aux['id_company'] = df['company'].apply(hash_md5)
    df_aux['name'] = df['company']
    df_aux['process_timestmap']  = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S"))
    df_aux['start_timestmap'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S"))
    df_aux['end_timestamp'] = None
    df_aux['creation_source_value'] = 'consoles.csv'
    df_aux['creation_source_desc'] = 'Archivo CSV'
    df_aux['update_timestamp'] = None
    return df_aux

def generate_dm_game(df):
    columns = ['id_game','name','description','process_timestamp','start_timestamp','end_timestamp','creation_source_value','creation_source_desc','update_timestamp']
    df = pd.DataFrame(df['name'].unique(),columns=['name'])
    df_aux = pd.DataFrame(columns = columns)
    df_aux['id_game'] = df['name'].apply(hash_md5)
    df_aux['name'] = df['name']
    df_aux['process_timestmap']  = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S"))
    df_aux['start_timestmap'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S"))
    df_aux['end_timestamp'] = None
    df_aux['creation_source_value'] = 'result.csv'
    df_aux['creation_source_desc'] = 'Archivo CSV'
    df_aux['update_timestamp'] = None
    return df_aux


def generate_ft_critics(df, df2):

    columns = ['id_critic','id_company','id_console','id_game','description','process_timestamp','start_timestamp','end_timestamp','creation_source_value','creation_source_desc','update_timestamp']
    df_aux = pd.DataFrame(columns = columns)
    df_aux['id_game'] = df['name'].apply(hash_md5)
    df_aux['id_console'] = df['console'].apply(hash_md5)
    df_aux['metascore'] = df['metascore']
    df_aux['userscore'] = df['userscore']
    df_company = pd.merge(df, df2, on=['console', 'console'], how = 'left')
    df_aux['id_company'] = df_company['company'].apply(hash_md5)
    df_aux['date'] = df['date'].astype('datetime64[ns]')
    df_aux['process_timestmap']  = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S"))
    df_aux['start_timestmap'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S"))
    df_aux['end_timestamp'] = None
    df_aux['creation_source_value'] = 'result.csv'
    df_aux['creation_source_desc'] = 'Archivo CSV'
    df_aux['update_timestamp'] = None
    return df_aux