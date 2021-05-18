import requests
import pandas as pd

# extract currency exchange rate data
access_key = "869c331656e9baaca09fad9d9dce9fe1"

url = "http://api.exchangeratesapi.io/v1/latest?base=EUR&access_key=" + access_key

response = requests.get(url)

# get currency and exchange rates
exchange_rate_html = pd.read_json(response.text)
exchange_rate = exchange_rate_html[["rates"]]

# rename columns
exchange_rate.columns = ["rate"]

# load data into csv
exchange_rate.to_csv('files/exchange_rate.csv')
