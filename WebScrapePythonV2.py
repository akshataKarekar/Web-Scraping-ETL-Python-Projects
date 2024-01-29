from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 


url='https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Bank", "Market_Cap_billions"]
db_name = 'BankDB.db'
table_name = 'Banks_by_Cap'
csv_path = './exchange_rate.csv'


# Code for ETL operations on Bank-Cap data

# Importing the required libraries

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
		
        if len(col)!=0:
                data_dict = {"Bank": col[1].find_all('a')[1].contents[0],
                             "Market_Cap_billions":float(col[2].contents[0][:-1])}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df
	
def transform(df):
	df['MC_GBP_Billion']=[np.round(x*0.8,2) for x in df['Market_Cap_billions']]
	df['MC_EUR_Billion']=[np.round(x*0.93,2) for x in df['Market_Cap_billions']]
	df['MC_INR_Billion']=[np.round(x*82.95,2) for x in df['Market_Cap_billions']]
	return df
	
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
        
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('BankDB.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from Banks_by_Cap WHERE Market_Cap_billions >= 100"
run_query(query_statement, sql_connection)


log_progress('Process Complete.')

sql_connection.close()
	
	
	