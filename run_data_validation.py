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

figi_id_length = """
    SELECT *
    FROM stock_market
    WHERE LENGTH(figi_identifier) < 12
"""

last_stock_value = """
    SELECT *
    FROM stock_market
    WHERE last_gbp = 0
"""

null_value = """
    SELECT *
    FROM stock_market
    WHERE symbol is NULL
    OR description is NULL
    OR last_gbp is NULL
    OR chg is NULL
    OR rating is NULL
    OR vol is NULL
    OR mkt_cap_gbp is NULL
    OR figi_identifier is NULL
    OR mic is NULL
    OR security_type is NULL
"""

rating_value = """
    SELECT *
    FROM stock_market
    WHERE rating NOT IN (
        'Buy',
        'Sell',
        'Strong Buy',
        'Strong Sell',
        'Neutral'
        )
"""

security_type_value = """
    SELECT *
    FROM stock_market
    WHERE security_type NOT IN (
        'ADR',
        'Closed-End Fund',
        'Common Stock',
        'ETP',
        'Ltd Part',
        'MLP',
        'NY Reg Shrs',
        'REIT',
        'Royalty Trst',
        'Unit'
        )
"""

queries = {
    figi_id_length: 'figi_id_length',
    last_stock_value: 'last_stock_value',
    null_value: 'null_value',
    rating_value: 'rating_value',
    security_type_value: 'security_type_value'
}


def save_as_txt(result, file_name):
    result.to_csv('files/' + file_name + '.txt', sep = '\t', index = False)

# run queries and save results txt
def validate():
    for query, file_name in queries.items():
        data = pd.read_sql(query, con = engine)
        if not data.empty:
            save_as_txt(data, file_name)