from bs4 import BeautifulSoup
import requests
import pandas as pd

url = "https://en.wikipedia.org/wiki/List_of_largest_banks"

html_data = requests.get(url).text
print(html_data[101:124])

# find all tables
soup = BeautifulSoup(html_data, "html5lib")
tables = soup.find_all('table')

# select table by market capitalization
market_cap_html = pd.read_html(str(tables[3]), flavor = 'bs4')[0]

# select bank name and market cap only
market_cap = market_cap_html[["Bank name", "Market cap(US$ billion)"]]
print(market_cap.head())

# load data into json
market_cap.to_json(r'bank_market_cap.json')