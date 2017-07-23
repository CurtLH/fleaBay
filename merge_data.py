#!/usr/bin/env python
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

# create a connection to get data from database
engine = create_engine('postgresql://postgres:apassword@localhost:5432/postgres')

# get API data from the database
api_df = pd.read_sql('ebay_api', con=engine)

# zap column names into lowercase
api_df.columns = [col.lower() for col in api_df.columns]

# reduce df to variables of interest
api_df = api_df[['itemid', 'condition_conditiondisplayname', 'listinginfo_endtime', 'sellingstatus_currentprice_value', 'sellingstatus_sellingstate']]

# rename columns
api_df = api_df.rename(columns={'condition_conditiondisplayname' : 'condition',
                                'listinginfo_endtime' : 'endtime',
                                'sellingstatus_currentprice_value' : 'price',
                                'sellingstatus_sellingstate' : 'status'})

# get item attribute info from the database
item_df = pd.read_sql('ebay_item_attr', con=engine)

# reduce df to variables of interest
item_df = item_df[['itemId', 'Mileage', 'Year', 'CYL', 'TITLE', 'TRANS', 'TRIM', 'EXT_COLOR']]

# reduce df to the years of interests
item_df = item_df[item_df['Year'] >= 2010]
print("Number of Camaros between 2010 and 2017: {}".format(len(item_df)))

# convert column names to lowercase
item_df.columns = [x.lower() for x in item_df.columns]

# for some reason, ads before Jan 18 were all sold ads
df.drop(df[df['endtime'] < '2017-01-18'].index, inplace=True)
df.drop(df[df['itemid'] == 122319430122].index, inplace=True)
df.drop(df[df['itemid'] == 322439374219].index, inplace=True)
df.drop(df[df['itemid'] == 192121219541].index, inplace=True)
df.drop(df[df['itemid'] == 222418277892].index, inplace=True)
df.drop(df[df.endtime.dt.year <= 2016].index, inplace=True)

# create a connection to write df to database
engine = create_engine('postgresql://postgres:apassword@localhost:5432/postgres')

# load the dataframe into the database
df.to_sql(name='ebay_merged', con=engine, if_exists = 'replace', chunksize=2500, index=False)

