# Finance ETL

ETL pipeline to extract data from multiple file formats, transform it into specific datatypes, and load it into a single source for analysis.

### Extract

First we extract US stock market data from TradingView with BeautifulSoup.

Then we extract stock symbols from Finnhub Stock API and exchange rates from Exchange Rates API.

### Transform

All .csv files with stock market data are validated, transformed and merged into a single file. 

Then joined with .json files with stock symbols and exchange rates.

### Load

Once the extracting and transforming process is complete, the stock data is loaded into the MySQL database.

All events are recorded in a log file.

### Schedule

Schedule and automation of data loading via Apache Airflow.

### Testing

Testing implemented with Pytest.
