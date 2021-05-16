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
    assert response.headers["Content-Type"] == "application/json"

def test_get_exchange_rate_check_data1():
    response = requests.get(url)
    response_body = response.json()
    assert response_body["country"] == "United States"

def test_get_exchange_rate_check_data2():
    response = requests.get(url)
    response_body = response.json()
    assert response_body["places"][0]["place name"] == "Beverly Hills"

def test_get_exchange_rate_check_data3():
    response = requests.get(url)
    response_body = response.json()
    assert len(response_body["places"]) == 1