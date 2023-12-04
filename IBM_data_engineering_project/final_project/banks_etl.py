# Importing the required libraries
import requests
from bs4 import BeautifulSoup as bs
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime



def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    now = datetime.now()
    timestamp = now.strftime('%Y-%h-%d-%H:%M:%S')

    with open(log_file, 'a+') as f:
        f.write(timestamp + " : " + message + "\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    df = pd.DataFrame(columns=table_attribs)

    html_page = requests.get(url).text
    html = bs(html_page, 'html.parser')

    tables = html.find_all('tbody')
    rows =  tables[0].find_all("tr") # extract table 1 rows

    for row in rows:
        cells = row.find_all('td')
        if len(cells) != 0:
            # print()
            dict = {
                "Name" : cells[1].contents[2].get('title'),
                "MC_USD_Billion" : float(cells[2].contents[0].strip('\n'))
            }

            df1 = pd.DataFrame(dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    rates = pd.read_csv(csv_path)
    rates_dict = rates.set_index('Currency').to_dict()['Rate']

    # convert numbers based on exchange rates
    df['MC_EUR_Billion'] = [np.round(x*rates_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x*rates_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*rates_dict['INR'],2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path, index = False)



def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name,sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    ''' Here, you define the required entities and call the relevant
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''

    output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(output)

## main program

# initialise variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
out_csv = "outputs/banks_data.csv"
db_name = "outputs/banks.db"
table_name = "Largest_banks"
log_file = "etl.log"
table_attribs = ['Name', 'MC_USD_Billion']
exchange_rates = "inputs/exchange_rate.csv"

log_progress("Preliminaries complete. Initiating ETL process")


df = extract(url,table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")


df = transform(df,exchange_rates)
log_progress("Data transformation complete. Initiating Loading process")


load_to_csv(df, out_csv)
log_progress("Data saved to CSV file")

#db connection
con = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(df,con, table_name)
log_progress("SQL Data loaded to Database as a table, Executing queries")

#queries
query1 = f"SELECT * FROM {table_name}"
query2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
query3 = f"SELECT Name from {table_name} LIMIT 5"

run_query(query1,con)
run_query(query2,con)
run_query(query3,con)
log_progress("Process Complete")

# Close SQLite3 connection
log_progress("Server Connection closed")
