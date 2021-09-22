import pandas as pd
from sqlalchemy import create_engine
import os
from sqlalchemy.orm import scoped_session, sessionmaker


root_path = os.path.abspath(__file__).split('/main.py')[0]

# Read Input File
input_file = open(f'{root_path}/input.txt', "r")
csv_file = open(f'{root_path}/equity.csv', "w+")
for each_line in input_file:
    csv_file.write(each_line.replace("|", ","))



# open file as Dataframe
df = pd.read_csv(f'{root_path}/equity.csv', header=None)
# Add Constants
df[54] = "OPTSTK"
df[55] = 1

# Update Col Header Names
column_name_as_indx = {0: "token", 1: "symbol", 2: "series", 21: "security_desc", 53: "isin_code", 54:"instrument_name", 55:"market_segment_id"}
new_df = df[column_name_as_indx.keys()]
new_df.rename(columns = column_name_as_indx, inplace = True)


new_df.to_csv(f'{root_path}/final.csv', index=False)


# Connect to Postgres
# engine = create_engine('postgresql://subarna:1234@localhost:5432/asthatrades') // Local
engine = create_engine('postgresql://postgres:1234@localhost:5432/postgres')

from sqlalchemy.types import VARCHAR, INTEGER
dtype = {
    "token":INTEGER,
    "symbol":VARCHAR,
    "series":VARCHAR,
    "security_desc":VARCHAR,
    "isin_code":VARCHAR,
    "instrument_name":VARCHAR,
    "market_segment_id":INTEGER,
}

db = scoped_session(sessionmaker(bind=engine, autocommit=True))

try:
    new_df.to_sql(name = 'equity_data', con = engine, index=False, dtype=dtype)

except Exception as Ei:
    print("\n[ EXCEPTION ] : Ei", Ei)

    try:
        new_df.to_sql(name = 'equity_data_temp', con = engine, index=False, dtype=dtype)
    except Exception as Eii:
        db.execute("DROP TABLE equity_data_temp;")
        print("\n[ EXCEPTION ] Eii: ", Eii)
        new_df.to_sql(name = 'equity_data_temp', con = engine, index=False, dtype=dtype)
    
    # RENAME prev table > new 
    try:
        db.execute("ALTER TABLE equity_data RENAME TO equity_data_new;")
    except Exception as Eiii:
        print("\n[ EXCEPTION ] Eiii: ", Eiii)
    
    # RENAME temp table > prev table 
    try:
        db.execute("ALTER TABLE equity_data_temp RENAME TO equity_data;")
    except Exception as Eiv:
        print("\n[ EXCEPTION ] Eiv: ", Eiv)

    # DROP new TABLE

    try:
        db.execute("DROP TABLE equity_data_new;")
    except Exception as Ev:
        print("\n[ EXCEPTION ] Ev: ", Ev)

# CREATE INDEX
try:
    db.execute("CREATE INDEX equity_data_ind_symbol ON equity_data(symbol);")
except Exception as Evi:
    print("\n[ EXCEPTION ] Evi: ", Evi)

print("Successfully Done.!!")
