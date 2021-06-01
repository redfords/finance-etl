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

- US stock market data is extracted from TradingView with BeautifulSoup and stored in .json files.
- Stock symbol data is collected from Finnhub Stock API and stored in a .csv file.
- Exchange rate data is collected from Exchange Rates API and stored in a .csv file.
- All stock market files are transformed and combined in a single file.
- ETL job execution is complete once data is loaded into the database and validated.

DAG graph view:

![Dag](https://i.imgur.com/krPainR.png)
