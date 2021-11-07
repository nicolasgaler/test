#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov  5 17:16:40 2021

@author: nico
"""


import requests
from io import StringIO
import pandas as pd
from sqlalchemy import create_engine
import json

RESULTS = 4500
USER='user'
PASSWORD='pass'
HOST='host'
DATABASE='db'
MY_NAME = 'nicolas_galer'

EXERCICE_5_QUERY = '''

    SELECT male.* 
    FROM (
            SELECT *
            FROM nicolas_galer_test_male
            ORDER BY registered_date DESC
            LIMIT 20
            ) AS male
    
    UNION ALL 
    
    SELECT female.* 
    FROM (
            SELECT *
            FROM nicolas_galer_test_female
            ORDER BY registered_date DESC
            LIMIT 20
            ) AS female
    '''

EXERCICE_6_QUERY = "SELECT * FROM "


def random_users_request (result):
  # Get Request from Random User Generator as bit value
  url = r'https://randomuser.me/api/?results=' + str(result) + '&format=csv' 
  r = requests.get(url)
  return r.content
  
def bit_to_pandas(bit):
    # Convert Bit Value to Pandas DF
    data = StringIO(bit.decode("iso-8859-1"))
    df = pd.read_csv(data, sep=",")
    df.columns = df.columns.str.replace(".", "_")
    return df

def create_table_from_dataframe(df,tableName):
    # Writes pandas DF to SQL
    engine = create_engine("mysql+pymysql://" + USER + ":" + PASSWORD + "@" + HOST + "/" + DATABASE)    
    df.to_sql(tableName, con=engine, if_exists='replace')
    print('Table ' + tableName + ' created')

def sql_query_to_pandas(query):
    # Writes SQL query to Pandas
    engine = create_engine("mysql+pymysql://" + USER + ":" + PASSWORD + "@" + HOST + "/" + DATABASE)    
    connection = engine.connect()
    execution = connection.execute(query)
    df = pd.DataFrame(execution.fetchall())
    connection.close()
    return df

def gender_split(df):
    # Split dataframe in male and female
    male_df = df[df['gender'] == 'male']
    female_df = df[df['gender'] == 'female']
    return male_df , female_df

def pandas_to_json_file(df,fileName):
    # Convert dataframe to json and save locally
    result_concat_json = df.to_json(orient='records')
    json_parsed = json.loads(result_concat_json)
    with open(fileName, 'w', encoding='utf-8') as f:
        json.dump(json_parsed, f, ensure_ascii=False, indent=4)

def __main__():
    # Exercise 1
    user_generator = random_users_request(RESULTS)
    main_df = bit_to_pandas(user_generator)
    
    # Exercise 2
    male_df , female_df = gender_split(main_df)
    create_table_from_dataframe(male_df, MY_NAME + '_test_male')
    create_table_from_dataframe(female_df, MY_NAME + '_test_female')
    
    # Exercise 3 and 4
    for i in range(0,100,10):
        range_df = main_df[main_df['dob_age'].between(i, 10+i,inclusive='left')]
        create_table_from_dataframe(range_df, MY_NAME + '_test_' + str(int(i/10)) )
    
    # Exercise 5
    result_df = sql_query_to_pandas(EXERCICE_5_QUERY)
    del result_df[0]
    create_table_from_dataframe(result_df,MY_NAME + '_test_20')
    
    # Exercise 6
    result_df_20 = sql_query_to_pandas(EXERCICE_6_QUERY + MY_NAME + '_test_20')
    result_df_5 = sql_query_to_pandas(EXERCICE_6_QUERY + MY_NAME + '_test_5')
    result_concat =  pd.concat([result_df_20,result_df_5]).drop_duplicates().reset_index(drop=True)
    del result_concat[0]
    result_concat.columns = main_df.columns
    pandas_to_json_file(result_concat,'ex_6.json')
    
    # Exercise 7
    result_df_20 = sql_query_to_pandas(EXERCICE_6_QUERY + MY_NAME + '_test_20')
    result_df_2 = sql_query_to_pandas(EXERCICE_6_QUERY + MY_NAME + '_test_2')
    result_concat =  pd.concat([result_df_20,result_df_2]).reset_index(drop=True)
    del result_concat[0]
    result_concat.columns = main_df.columns
    pandas_to_json_file(result_concat,'ex_7.json')

__main__()
