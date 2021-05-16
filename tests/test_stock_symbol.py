import pytest
import requests
import stock_symbol

token = "c2da452ad3i9v1gkgueg"

url = "https://finnhub.io/api/v1/stock/symbol?exchange=US&token=" + token

def test_get_symbol_status_code():
    response = requests.get(url)
    assert response.status_code == 200

def test_get_symbol_content_type():
    response = requests.get(url)
    assert response.headers["Content-Type"] == "application/json; charset=utf-8"

def test_get_symbol_check_attributes():
    response = requests.get(url)
    attributes = list(response.json()[0].keys())
    keys = ['currency', 'description', 'displaySymbol', 'figi', 'mic', 'symbol', 'type']
    assert attributes == keys