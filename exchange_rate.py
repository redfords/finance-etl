import requests
import pandas as pd

# extract currency exchange rate data
access_key = "869c331656e9baaca09fad9d9dce9fe1"

url = "http://api.exchangeratesapi.io/v1/latest?base=EUR&access_key=" + access_key

response = requests.get(url)

# get currency and exchange rates
exchange_rate_html = pd.read_json(response.text)
exchange_rate = exchange_rate_html[["rates"]]

# raname columns
exchange_rate.columns = ["Rate"]
print(exchange_rate)

# load data into csv
exchange_rate.to_csv('files/exchange_rates.csv')
