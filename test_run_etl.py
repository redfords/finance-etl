import pytest
import pandas as pd
import numpy
import run_etl

def test_extract():
    data = run_etl.extract()

    data_type = {
    'Company': 'object',
    'Last (US$)': 'float64',
    'CHG%': 'object',
    'CHG': 'float64',
    'Rating': 'object',
    'Vol': 'object',
    'Mkt Cap (US$)':'object'
    }

    index = ['Company', 'Last (US$)', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap (US$)']
    test_series = pd.Series(data = data_type, index = index)

    assert data.dtypes.equals(test_series)

def test_extract_index():
    data = run_etl.extract()

    assert data.index.is_monotonic_increasing

def test_extract_from_stock_symbol():
    data = run_etl.extract_from_stock_symbol()

    data_type = {
    'Currency': 'object',
    'Description': 'object',
    'Symbol': 'object',
    'FIGI Identifier': 'object',
    'MIC': 'object',
    'Security Type': 'object'
    }

    index = ["Currency", "Description", "Symbol", "FIGI Identifier", "MIC", "Security Type"]
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
    'Symbol': 'object',
    'Description': 'object',
    'Last (GBP$)': 'float64',
    'CHG%': 'object',
    'CHG': 'float64',
    'Rating': 'object',
    'Vol': 'object',
    'Mkt Cap (GBP$)':'object',
    'FIGI Identifier': 'object',
    'MIC': 'object',
    'Security Type': 'object'
    }

    index = [
        'Symbol',
        'Description',
        'Last (GBP$)',
        'CHG%',
        'CHG',
        'Rating',
        'Vol',
        'Mkt Cap (GBP$)',
        'FIGI Identifier',
        'MIC',
        'Security Type'
    ]
    test_series = pd.Series(data = data_type, index = index)

    assert data.dtypes.equals(test_series)

def test_transform_index():
    extracted_data = run_etl.extract()
    exchange_rate = run_etl.find_exchange_rate()
    stock_symbol_data = run_etl.extract_from_stock_symbol()

    data = run_etl.transform(extracted_data, exchange_rate, stock_symbol_data)

    assert data.index.is_monotonic_increasing
