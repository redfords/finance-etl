import pytest
import pandas as pd
import numpy
import run_etl

def test_extract():
    data = run_etl.extract()

    data_type = {
    'company': 'object',
    'last_usd': 'float64',
    'chg_p': 'object',
    'chg': 'float64',
    'rating': 'object',
    'vol': 'object',
    'mkt_cap_usd':'object'
    }

    index = ['company', 'last_usd', 'chg_p', 'chg', 'rating', 'vol', 'mkt_cap_usd']
    test_series = pd.Series(data = data_type, index = index)

    assert data.dtypes.equals(test_series)

def test_extract_index():
    data = run_etl.extract()

    assert data.index.is_monotonic_increasing

def test_extract_from_stock_symbol():
    data = run_etl.extract_from_stock_symbol()

    data_type = {
    'currency': 'object',
    'description': 'object',
    'symbol': 'object',
    'figi_identifier': 'object',
    'mic': 'object',
    'security_type': 'object'
    }

    index = ["currency", "description", "symbol", "figi_identifier", "mic", "security_type"]
    test_series = pd.Series(data = data_type, index = index)

    assert data.dtypes.equals(test_series)

def test_find_exchange_rate():
    exchange_rate = run_etl.find_exchange_rate()

    assert isinstance(exchange_rate, numpy.float64)

def test_transform():
    extracted_data = run_etl.extract()
    exchange_rate = run_etl.find_exchange_rate()
    stock_symbol_data = run_etl.extract_from_stock_symbol()

    data = run_etl.transform(extracted_data, exchange_rate, stock_symbol_data)

    data_type = {
    'symbol': 'object',
    'description': 'object',
    'last_gbp': 'float64',
    'chg_p': 'object',
    'chg': 'float64',
    'rating': 'object',
    'vol': 'object',
    'mkt_cap_gbp':'object',
    'figi_identifier': 'object',
    'mic': 'object',
    'security_type': 'object'
    }

    index = [
        'symbol',
        'description',
        'last_gbp',
        'chg_p',
        'chg',
        'rating',
        'vol',
        'mkt_cap_gbp',
        'figi_identifier',
        'mic',
        'security_type'
    ]
    test_series = pd.Series(data = data_type, index = index)

    assert data.dtypes.equals(test_series)

def test_transform_index():
    extracted_data = run_etl.extract()
    exchange_rate = run_etl.find_exchange_rate()
    stock_symbol_data = run_etl.extract_from_stock_symbol()

    data = run_etl.transform(extracted_data, exchange_rate, stock_symbol_data)

    assert data.index.is_monotonic_increasing
