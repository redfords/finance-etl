# Finance ETL

ETL pipeline to extract data from multiple file formats, transform it into specific datatypes, and load it into a single source for analysis.

## Modules

- Requests
- BeautifulSoup
- Pandas
- Numpy
- SQLAlchemy
- Pytest
- Apache Airflow

## ETL Flow

General overview:

- US stock market data extracted from TradingView with BeautifulSoup and stored in .json files.
- Stock symbols extracted from Finnhub Stock API and exchange rates from Exchange Rates API, then stored in .csv files.
- All files with stock market data are transformed and merged into a single file, then joined with the stock symbol and exchange rate.
- The stock market data is loaded into the database and validated.
- All events are recorded in a log file

DAG graph view:

![Dag](https://i.imgur.com/krPainR.png)
