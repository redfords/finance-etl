import pandas as pd
from sqlalchemy import create_engine

# connect to database
engine = create_engine(
    "mysql+pymysql://{user}:{pw}@{host}/{db}"
    .format(
        user = "root",
        pw = "password",
        host = "localhost:33060",
        db = "test"
    )
)

# drop table if exists
query = "DROP TABLE if EXISTS stock_market;"
engine.execute(query)

# create table stock_market
query = """
    CREATE TABLE if NOT EXISTS stock_market (
    stock_id INT NOT NULL AUTO_INCREMENT,
    symbol VARCHAR(10),
    description VARCHAR(100),
    last_gbp FLOAT(10, 3),
    chg FLOAT(6, 2),
    rating VARCHAR(15),
    vol VARCHAR(10),
    mkt_cap_gbp VARCHAR(10),
    figi_identifier CHAR(12),
    mic CHAR(4),
    security_type VARCHAR(15),
    PRIMARY KEY (stock_id)
    );
"""
engine.execute(query)

# insert .csv into db
# create data frame from .csv
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)

    return dataframe

# insert .csv files into db
def insert_into_db(file_to_process, table):
    data = extract_from_csv('files/' + file_to_process + '.csv')
    data = data.iloc[:, 1:]

    data.to_sql(table, con = engine, if_exists = 'append', index = False)

#insert_into_db('file', 'table')