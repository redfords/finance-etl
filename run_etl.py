from datetime import datetime
import insert_table
import pandas as pd
pd.options.mode.chained_assignment = None

tmpfile = "files/temp.tmp"
logfile = "files/logfile.txt"
targetfile = "files/transformed_data.csv"

# files to convert from .json to dataframe
file_name = [
    'large_cap',
    'top_gainers',
    'top_losers',
    'most_active',
    'most_volatile',
    'overbought',
    'oversold'
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
        extracted_data = extracted_data.append(
            extract_from_json('files/' + file + '.json'), ignore_index = True)

    extracted_data.columns = [
        'company', 'last_usd', 'chg_p', 'chg', 'rating', 'vol', 'mkt_cap_usd']

    return extracted_data

def extract_from_stock_symbol():
    data = pd.DataFrame(columns = [
        "currency", "description", "symbol", "figi_identifier", "mic", "security_type"])
    csv_data = extract_from_csv('files/stock_symbol.csv')
    data = csv_data.iloc[:, 1:7]

    return data

def find_exchange_rate():
    data = pd.DataFrame()
    data = extract_from_csv('files/exchange_rate.csv')
    data.columns = ['currency', 'rate']
    data.set_index('currency', inplace = True)

    exchange_rate = data.at['GBP', 'rate']
    
    return exchange_rate

def transform(data, exchange_rate, stock_symbol):
    # remove duplicates
    data = data.drop_duplicates(
        subset = ['company', 'last_usd', 'chg_p', 'chg', 'rating', 'vol', 'mkt_cap_usd'])

    # split company into symbol and description
    data[['symbol', 'description']] = data['company'].str.split(' ', 1, expand = True)
    data.drop('company', axis = 1, inplace = True)

    # rearrange columns
    data = data[[
        'symbol', 'description', 'last_usd', 'chg_p', 'chg', 'rating', 'vol', 'mkt_cap_usd']]

    # convert exchange rate from USD to GBP
    data['last_usd'] = pd.to_numeric(data['last_usd'])
    data['last_usd'] = round(data['last_usd'] * exchange_rate, 3)
    # data['mkt_cap_usd'] = round(data['mkt_cap_usd'] * exchange_rate, 3)

    # rename columns to GBP
    data.columns = [
        'symbol', 'description', 'last_gbp', 'chg_p', 'chg', 'rating', 'vol', 'mkt_cap_gbp']

    # sort by stock value
    data = data.sort_values(by = ['last_gbp'], ascending = [False])
    data = data.reset_index(drop = True)

    # add FIGI identifier, market identifier code and security type
    data = pd.merge(data, stock_symbol[['symbol', 'figi_identifier', 'mic', 'security_type']],
        on = 'symbol', how = 'left')

    # replace nan values
    values = {'symbol': '-', 'figi_identifier': '-', 'security_type': '-' }
    data.fillna(value = values, inplace = True)

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
        f.write('[' + timestamp + ']' + ' ' + message + '\n')

# running the ETL process
log("ETL job started")

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
insert_table.insert_into_db('transformed_data', 'stock_market')
log("Load phase ended")

log("ETL job ended")