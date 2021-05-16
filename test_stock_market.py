import pytest
import requests
import pandas as pd
import stock_market

def test_extract_data():
    url = "https://www.tradingview.com/markets/stocks-usa/market-movers-"

    extract = stock_market.extract_data(url)

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

    assert extract.dtypes.equals(test_series)



