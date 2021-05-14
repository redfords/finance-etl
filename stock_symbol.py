import requests
import pandas as pd

# extract stock symbol data
token = "c2da452ad3i9v1gkgueg"

url = "https://finnhub.io/api/v1/stock/symbol?exchange=US&token=" + token

r = requests.get(url)

# get symbol and figi identifier
stock_symbol = pd.read_json(r.text)
stock_symbol = stock_symbol[["currency", "description", "displaySymbol", "figi", "mic", "type"]]

# raname columns
stock_symbol.columns = [
    "Currency", "Description", "Symbol", "FIGI Identifier", "MIC", "Security Type"]

# load data into csv
stock_symbol.to_csv('stock_symbol.csv') 
