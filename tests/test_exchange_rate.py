import pytest
import requests
import exchange_rate

access_key = "869c331656e9baaca09fad9d9dce9fe1"

url = "http://api.exchangeratesapi.io/v1/latest?base=EUR&access_key=" + access_key

def test_get_exchange_rate_status_code():
    response = requests.get(url)
    assert response.status_code == 200

def test_get_exchange_rate_content_type():
    response = requests.get(url)
    assert response.headers["Content-Type"] == "application/json; Charset=UTF-8"

def test_get_exchange_rate_check_base():
    response = requests.get(url)
    response_body = response.json()
    assert response_body["base"] == "EUR"
