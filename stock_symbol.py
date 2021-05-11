import requests
import pandas as pd

# extract stock symbol data
url = "https://finnhub.io/api/v1/stock/symbol?exchange=US&token=c2da452ad3i9v1gkgueg"

r = requests.get(url)

# get symbol and figi identifier
stock_symbol = pd.read_json(r.text)
stock_symbol = stock_symbol[["currency", "description", "displaySymbol", "figi"]]

# raname columns
stock_symbol.columns = ["Currency", "Description", "Symbol", "FIGI Identifier"]
print(stock_symbol.head())

# load data into csv
stock_symbol.to_csv('stock_symbol.csv')
