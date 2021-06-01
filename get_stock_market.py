from bs4 import BeautifulSoup
import requests
import pandas as pd

"""
Tables to extract:

Largest companies by market capitalization
Stocks that have increased the most in price
Stocks that have lost the most value
Stocks that have been traded the most
Stocks with the highsest volatility
Overvalued stocks
Undervalued stocks
"""

url = "https://www.tradingview.com/markets/stocks-usa/market-movers-"

# extract stock market tables
def extract_data(url):
    html_data = requests.get(url).text
    soup = BeautifulSoup(html_data, "html5lib")
    tables = soup.find_all('table')

    data = pd.read_html(str(tables[0]), flavor = 'bs4')[0]
    data = data.iloc[:, 0:7]
    data.drop(data.columns[2], axis = 1, inplace = True)
    data.columns = ['company', 'last', 'chg', 'rating', 'vol', 'mkt_cap']

    # remove space before company symbol
    data.loc[data['company'].str.contains(
    "^[A-Z][\s]", regex = True), 'company'] = data['company'].str[3:]

    return data

# convert table into .json file
def data_to_json(table_name, url_name):
    data = extract_data(url + url_name + '/')
    data.to_json('files/' + table_name + '.json')

# read list of tables
def read_file_list(tables_to_extract):
    for key, value in tables_to_extract.items():
        data_to_json(key, value)

# list of tables to extract
tables_to_extract = {
    'large_cap': 'large-cap',
    'top_gainers': 'gainers',
    'top_losers': 'losers',
    'most_active': 'active',
    'most_volatile': 'most-volatile',
    'overbought': 'overbought',
    'oversold': 'oversold'
}

read_file_list(tables_to_extract)