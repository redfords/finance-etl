from datetime import datetime
import pandas as pd
pd.options.mode.chained_assignment = None

tmpfile = "files/temp.tmp"
logfile = "files/logfile.txt"
targetfile = "files/transformed_data.csv"

# files to convert from .json to dataframe
file_name = [
    'files/large_cap.json',
    'files/top_gainers.json',
    'files/top_losers.json',
    'files/most_active.json',
    'files/most_volatile.json',
    'files/overbought.json',
    'files/oversold.json'
]

# extract CSV to dataframe
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)

    return dataframe

# extract .json to dataframe
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)

    return dataframe

# merge all .json files to dataframe
def extract():
    extracted_data = pd.DataFrame()

    for file in file_name:
        extracted_data = extracted_data.append(extract_from_json(file), ignore_index = True)

    extracted_data.columns = [
        'Company', 'Last (US$)', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap (US$)']

    return extracted_data

def extract_from_stock_symbol():
    data = pd.DataFrame(columns = [
        "Currency", "Description", "Symbol", "FIGI Identifier", "MIC", "Security Type"])
    csv_data = extract_from_csv('files/stock_symbol.csv')
    data = csv_data.iloc[:, 1:7]

    return data

def find_exchange_rate():
    data = pd.DataFrame()
    data = extract_from_csv('files/exchange_rates.csv')
    data.columns = ['Currency', 'Rate']
    data.set_index('Currency', inplace = True)

    exchange_rate = data.at['GBP', 'Rate']
    
    return exchange_rate

def transform(data, exchange_rate, stock_symbol):
    # remove duplicates
    data = data.drop_duplicates(
        subset = ['Company', 'Last (US$)', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap (US$)'])

    # split company into symbol and description
    data[['Symbol', 'Description']] = data['Company'].str.split(' ', 1, expand = True)
    data.drop('Company', axis = 1, inplace = True)

    # rearrange columns
    data = data[[
        'Symbol', 'Description', 'Last (US$)', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap (US$)']]

    # convert exchange rate from USD to GBP
    data['Last (US$)'] = pd.to_numeric(data['Last (US$)'])
    data['Last (US$)'] = round(data['Last (US$)'] * exchange_rate, 3)
    # data['Mkt Cap (US$)'] = round(data['Mkt Cap (US$)'] * exchange_rate, 3)

    # rename columns to GBP
    data.columns = [
        'Symbol', 'Description', 'Last (GBP$)', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap (GBP$)']

    # sort by stock value
    data = data.sort_values(by = ['Last (GBP$)'], ascending = [False])
    data = data.reset_index(drop = True)

    # add FIGI identifier, market identifier code and security type
    data = pd.merge(data, stock_symbol[['Symbol', 'FIGI Identifier', 'MIC', 'Security Type']],
        on = 'Symbol', how = 'left')

    return data

# loading
def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile)

# logging
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    
    with open("files/logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')

# running the ETL process
# log("ETL job started")

log("Extract phase started")
extracted_data = extract()
exchange_rate = find_exchange_rate()
stock_symbol_data = extract_from_stock_symbol()
log("Extract phase ended")

log("Transform phase started")
transformed_data = transform(extracted_data, exchange_rate, stock_symbol_data)
log("Transform phase ended")

log("Load phase started")
load(targetfile, transformed_data)
log("Load phase ended")

log("ETL job ended")
