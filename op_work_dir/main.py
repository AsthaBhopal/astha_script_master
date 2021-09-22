import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
# from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import time

root_path = os.path.abspath(__file__).split('/main.py')[0]

# Read Option File
input_file = open(f'{root_path}/input.txt', "r")
csv_file = open(f'{root_path}/option.csv', "w+")
for each_line in input_file:
    csv_file.write(each_line.replace("|", ","))
csv_file.close()
input_file.close()

# open file as Dataframe

df = pd.read_csv(f'{root_path}/option.csv', header=None)
print(df)


# column_name_as_indx = {0: "nToken", 2: "sInstrumentName", 3:"sSymbol", 6:"nExpiryDate1", 7:"nStrikePrice1", 8:"sOptionType",  53: "sSecurityDesc", add: "sISINCode", add:"nMarketSegmentId", }
column_name_as_indx = {0: "token", 2: "instrument_name", 3:"symbol", 6:"expiry_date", 7:"strike_price", 8:"option_type",  53: "security_desc"}
new_df = df[column_name_as_indx.keys()]
new_df.rename(columns = column_name_as_indx, inplace = True)
# add new constants
new_df['isin_code'] = np.nan
new_df['market_segment_id'] = 2

new_df['expiry_date'] = new_df['expiry_date'].apply(lambda x: time.strftime("%b", time.localtime(x)).upper()) 
new_df.to_csv(f'{root_path}/final.csv', index=False)


# Connect to Postgres
# engine = create_engine('postgresql://subarna:1234@localhost:5432/asthatrades') // Local
engine = create_engine('postgresql://postgres:1234@localhost:5432/postgres')

from sqlalchemy.types import VARCHAR, INTEGER
dtype = {
    "token":INTEGER,
    "instrument_name":VARCHAR,
    "symbol":VARCHAR,
    "expiry_date":VARCHAR,
    "strike_price":INTEGER,
    "option_type":VARCHAR,
    "security_desc":VARCHAR,
    "isin_code":VARCHAR,
    "market_segment_id":INTEGER,
}
db = scoped_session(sessionmaker(bind=engine, autocommit=True))


try:
    new_df.to_sql(name = 'option_data', con = engine, index=False, dtype=dtype)

except Exception as Ei:
    print("\n[ EXCEPTION ] : Ei", Ei)

    try:
        new_df.to_sql(name = 'option_data_temp', con = engine, index=False, dtype=dtype)
    except Exception as Eii:
        db.execute("DROP TABLE option_data_temp;")
        print("\n[ EXCEPTION ] Eii: ", Eii)
        new_df.to_sql(name = 'option_data_temp', con = engine, index=False, dtype=dtype)
    
    # RENAME prev table > new 
    try:
        db.execute("ALTER TABLE option_data RENAME TO option_data_new;")
    except Exception as Eiii:
        print("\n[ EXCEPTION ] Eiii: ", Eiii)
    
    # RENAME temp table > prev table 
    try:
        db.execute("ALTER TABLE option_data_temp RENAME TO option_data;")
    except Exception as Eiv:
        print("\n[ EXCEPTION ] Eiv: ", Eiv)

    # DROP new TABLE

    try:
        db.execute("DROP TABLE option_data_new;")
    except Exception as Ev:
        print("\n[ EXCEPTION ] Ev: ", Ev)

# CREATE INDEX
try:
    db.execute("CREATE INDEX option_data_idx_symbol_strikeprice_optiontype_expirydate ON option_data(symbol, strike_price, option_type, expiry_date);")
    db.execute("CREATE INDEX option_data_idx_symbol_strikeprice_optiontype ON option_data(symbol, strike_price, option_type);")
    db.execute("CREATE INDEX option_data_idx_symbol_strikeprice_expirydate ON option_data(symbol, strike_price, expiry_date);")
    db.execute("CREATE INDEX option_data_idx_symbol_optiontype_expirydate ON option_data(symbol, option_type, expiry_date);")
    db.execute("CREATE INDEX option_data_idx_symbol_strikeprice ON option_data(symbol, strike_price);")
    db.execute("CREATE INDEX option_data_idx_symbol ON option_data(symbol);")
except Exception as Evi:
    print("\n[ EXCEPTION ] Evi: ", Evi)

# PUSH ISIS code from equity to option/future
try:
    db.execute("UPDATE option_data SET isin_code = s.isin_code FROM equity_data AS s WHERE option_data.symbol = s.symbol;")
except Exception as Evii:
    print("\n[ EXCEPTION ] Evii: ", Evii)
    
# Vector Index for Full Text Search
try:
    db.execute("DROP TABLE option_data_t;")
except Exception as E:
    print("[ EXCEPTION ] Table doesn't exist error: ", E)

try:
    new_df.to_sql(name = 'option_data_t', con = engine, index=False, dtype=dtype)
    db.execute("UPDATE option_data_t SET isin_code = s.isin_code FROM equity_data AS s WHERE option_data_t.symbol = s.symbol;")
    db.execute("ALTER TABLE option_data_t ADD COLUMN tsv_idx tsvector;")
    db.execute("UPDATE option_data_t SET tsv_idx = to_tsvector(symbol || ' ' || strike_price || ' ' || option_type || ' ' || expiry_date);")
    db.execute("CREATE INDEX tsv_idx ON option_data_t USING GIN(tsv_idx);")

except Exception as E:
    print("[ EXCEPTION ] Vector error :*check ", E)

print("Successfully Done.!!")
