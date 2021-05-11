from bs4 import BeautifulSoup
import requests
import pandas as pd

def extract_data(url):
    html_data = requests.get(url).text
    soup = BeautifulSoup(html_data, "html5lib")
    tables = soup.find_all('table')

    data = pd.read_html(str(tables[0]), flavor = 'bs4')[0]
    data = data.iloc[:, 0:7]
    data.columns = ['Company', 'Last', 'CHG%', 'CHG', 'Rating', 'Vol', 'Mkt Cap']

    return data

# largest companies by market cap
url = "https://www.tradingview.com/markets/stocks-usa/market-movers-large-cap/"
large_cap = extract_data(url)
large_cap.to_json(r'large_cap.json')

# stocks that have increased the most in price
url = "https://www.tradingview.com/markets/stocks-usa/market-movers-gainers/"
top_gainers = extract_data(url)
top_gainers.to_json(r'top_gainers.json')

# stocks that have lost the most value
url = "https://www.tradingview.com/markets/stocks-usa/market-movers-losers/"
top_losers = extract_data(url)
top_losers.to_json(r'top_losers.json')

# stocks that have been traded the most
url = "https://www.tradingview.com/markets/stocks-usa/market-movers-active/"
most_active = extract_data(url)
most_active.to_json(r'most_active.json')

# stocks with the highest volatility
url = "https://www.tradingview.com/markets/stocks-usa/market-movers-most-volatile/"
most_volatile = extract_data(url)
most_volatile.to_json(r'most_volatile.json')

# overvalued stocks
url = "https://www.tradingview.com/markets/stocks-usa/market-movers-overbought/"
overbought = extract_data(url)
overbought.to_json(r'overbought.json')

# undervalued stocks
url = "https://www.tradingview.com/markets/stocks-usa/market-movers-oversold/"
oversold = extract_data(url)
oversold.to_json(r'oversold.json')
