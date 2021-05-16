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
    assert response.headers["Content-Type"] == "application/json"

def test_get_symbol_check_data1():
    response = requests.get(url)
    response_body = response.json()
    assert response_body["country"] == "United States"

def test_get_symbol_check_data2():
    response = requests.get(url)
    response_body = response.json()
    assert response_body["places"][0]["place name"] == "Beverly Hills"

def test_get_symbol_check_data3():
    response = requests.get(url)
    response_body = response.json()
    assert len(response_body["places"]) == 1