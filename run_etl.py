import glob
import pandas as pd
from datetime import datetime

tmpfile = "temp.tmp"
logfile = "logfile.txt"
targetfile = "transformed_data.csv"

# extract CSV and json files

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)

    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines = True)

    return dataframe

# extract exchange rate

def find_exchange_rate(symbol, data):
    exchange_rate = data.at[symbol, 'Rate']

    return exchange_rate

def extract_from_exchange_rate():
    data = pd.DataFrame(columns = ['Rate'])
    data = extract_from_csv('exchange_rates.csv')

    exchange_rate = find_exchange_rate('GBP', data)

    return exchange_rate

# extract all files into data frame

def extract():
    extracted_data = pd.DataFrame(
        columns = ['Company', 'Last (US$)', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap (US$)'])

    for csvfile in glob.glob("*.json"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index = True)

    return extracted_data

def transform(data, exchange_rate):

    # change exchange rate from USD to GBP
    data['Last (US$)'] = round(data['Last'] * exchange_rate, 3)
    data['Mkt Cap (US$)'] = round(data['Mkt Cap (US$)'] * exchange_rate, 3)

    # rename column to GBP
    data.columns = ['Company', 'Last (GBP$)', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap (GBP$)']

    # sort by stock value
    data = data.sort_values(by = ['Last (GBP$)'], ascending = [False])
    
    # remove duplicates
    data = data.drop_duplicates(
        subset = ['Company', 'Last (GBP$)', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap (GBP$)'])

    return data

# loading

def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile)

# logging

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    
    with open("logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')

# running the ETL process

log("ETL job started")

log("Extract phase started")
extracted_data = extract()
exchange_rate = extract_from_exchange_rate
log("Extract phase ended")
print(extracted_data)

log("Transform phase started")
transformed_data = transform(extracted_data, exchange_rate)
log("Transform phase ended")
transformed_data

log("Load phase started")
load(targetfile, transformed_data)
log("Load phase ended")

log("ETL job ended")

