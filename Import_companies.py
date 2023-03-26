from snowflake.sqlalchemy import URL
import pandas as pd
from sqlalchemy import create_engine


engine = create_engine(URL(
    account='nua76068.us-east-1',
    user='BURHAND',
    password='Core@123',
    database='AB_INTENT_KW',
    schema='PIPELINE',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN'
))

source_file_location = (r"C:\Work\Audience Bridge\Round 6\companies_to_search_mv2.csv")
df = pd.read_csv(source_file_location,encoding ='latin1')
df = df.reset_index()
df["INDEX"] = df.index + 1
df = df[['INDEX','COMPANYNAME','COUNTRYCODE']]
print(df)

df.to_csv("C:\Work\Audience Bridge\Round 6\companies_to_search_mv2_updated.csv")



ext_file_location = (r"C:\Work\Audience Bridge\Round 6\companies_to_search_mv2_updated.csv") 
count = 1
skip_rows= 0
chunk_size=10000
# read data from extracted csv
for df in pd.read_csv(ext_file_location,encoding ='latin1',chunksize=chunk_size,skiprows=skip_rows,error_bad_lines=False):
    if count == 1:
        #df = df[['ID','COMPANYNAME','COUNTRYCODE']]
        df = df.rename(columns={'INDEX': 'ID'})[['ID', 'COMPANYNAME', 'COUNTRYCODE']]
        table_name = "comp_to_search"
        if_exists =  'replace'
        primary_key = "ID"
        connection = engine.connect()
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, index_label=None, chunksize=None, method=None)
        connection.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})")
        count += 1
        skip_rows += chunk_size
    else:
        #df = df[['ID','COMPANYNAME','COUNTRYCODE']]
        df = df.rename(columns={'INDEX': 'ID'})[['ID', 'COMPANYNAME', 'COUNTRYCODE']]
        table_name = "comp_to_search"
        if_exists =  'append'
        connection = engine.connect()
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, index_label=None, chunksize=None, method=None)
        skip_rows += chunk_size

connection.close()
engine.dispose()
print('Data successully imported')