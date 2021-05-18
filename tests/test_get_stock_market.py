import pytest
import requests
import pandas as pd
import get_stock_market

def test_extract_data():
    url = "https://www.tradingview.com/markets/stocks-usa/market-movers-"

    extract = get_stock_market.extract_data(url)

    data_type = {
    'company': 'object',
    'last': 'float64',
    'chg_p': 'object',
    'chg': 'float64',
    'rating': 'object',
    'vol': 'object',
    'mkt_cap':'object'
    }

    index = ['company', 'last', 'chg_p', 'chg', 'rating', 'vol', 'mkt_cap']
    test_series = pd.Series(data = data_type, index = index)

    assert extract.dtypes.equals(test_series)



