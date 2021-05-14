from bs4 import BeautifulSoup
import requests
import pandas as pd

"""
Tables to extract:

Largest companies by market capitalization
Stocks that have increased the most in price
Stocks that have lost the most value
Stocks that have been traded the most
Stocks with the highest volatility
Overvalued stocks
Undervalued stocks
"""

url = "https://www.tradingview.com/markets/stocks-usa/market-movers-"

def extract_data(url):
    html_data = requests.get(url).text
    soup = BeautifulSoup(html_data, "html5lib")
    tables = soup.find_all('table')

    data = pd.read_html(str(tables[0]), flavor = 'bs4')[0]
    data = data.iloc[:, 0:7]
    data.columns = ['Company', 'Last', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap']

    return data

def data_to_json(file_name, url_name):
    data = extract_data(url + url_name + '/')
    data.to_json(file_name + '.json')

# list of files to extract
file_name = {
    'large_cap': 'large-cap/',
    'top_gainers': 'gainers/',
    'top_losers': 'losers/',
    'most_active': 'active/',
    'most_volatile': 'most-volatile/',
    'overbought': 'overbought/',
    'oversold': 'oversold/'
}

for key, value in file_name.items():
    data_to_json(key, value)
