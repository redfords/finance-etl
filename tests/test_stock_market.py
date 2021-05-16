import pytest
import requests
import pandas as pd
import stock_market as sm

url = "https://www.tradingview.com/markets/stocks-usa/market-movers-"

def test_extract_data():

    extract = sm.extract_data(url)

    data_type = {
    'Company': 'object',
    'Last': 'float64',
    'CHG%': 'object',
    'CHG': 'float64',
    'Rating': 'object',
    'Vol': 'object',
    'Mkt Cap':'object'
    }

    index = ['Company', 'Last', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap']
    test_series = pd.Series(data = data_type, index = index)
    assert extract.dtypes == test_series

def test_data_to_json():
    pass

def test_read_file_list():
    pass
