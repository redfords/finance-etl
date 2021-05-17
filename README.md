# Finance ETL

ETL pipeline to extract data from multiple file formats, transform it into specific datatypes, and load it into a single source for analysis.

## Modules

- Pandas
- Numpy
- Requests
- BeautifulSoup
- Pytest
- SQLAlchemy
- Airflow

## Project

US stock market data extracted from TradingView with BeautifulSoup. Stock symbols extracted from Finnhub Stock API and exchange rates from Exchange Rates API.

All .csv files with stock market data are validated, transformed and merged into a single file. Then joined with .json files with stock symbols and exchange rates.

Once the extracting and transforming process is complete, the stock data is loaded into the MySQL database. All events are recorded in a log file.

Schedule and automation of data loading via Apache Airflow. Testing implemented with Pytest.
