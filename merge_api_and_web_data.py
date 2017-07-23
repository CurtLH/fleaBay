#!/usr/bin/env python

import logging
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import click

# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


@click.command()
def cli():

    """
    Load API data and web data and merge into one dataset
    """

    # connect to the databse
    try:
        engine = create_engine('postgresql://postgres:apassword@localhost:5432/postgres')
        logger.info("Successfully connected to the database")

    except:
        logger.info("Unable to connect to the database")

    # get API data from the database
    api_df = pd.read_sql('ebay_api', con=engine)

    # zap column names into lowercase
    api_df.columns = [col.lower() for col in api_df.columns]

    # reduce df to variables of interest
    api_df = api_df[['itemid', 
                     'condition_conditiondisplayname', 
                     'listinginfo_endtime', 
                     'sellingstatus_currentprice_value', 
                     'sellingstatus_sellingstate']]

    # rename columns
    api_df = api_df.rename(columns={'condition_conditiondisplayname' : 'condition',
                                    'listinginfo_endtime' : 'endtime',
                                    'sellingstatus_currentprice_value' : 'price',
                                    'sellingstatus_sellingstate' : 'status'})

    # get item attribute info from the database
    web_df = pd.read_sql('ebay_web', con=engine)

    # reduce df to variables of interest
    web_df = web_df[['itemId', 'Mileage', 'Year', 'CYL', 'TITLE', 'TRANS', 'TRIM', 'EXT_COLOR']]

    # reduce df to the years of interests
    web_df = web_df[web_df['Year'] >= 2010]
    logger.info("Number of Camaros between 2010 and 2017: {}".format(len(web_df)))

    # convert column names to lowercase
    web_df.columns = [x.lower() for x in web_df.columns]

    # merge api data and web data
    df = pd.merge(web_df, api_df, how='inner', on='itemid')
    logger.info("Number of records in merged dataset: {}".format(len(df)))

    # for some reason, ads before Jan 18 were all sold ads
    df.drop(df[df['endtime'] < '2017-01-18'].index, inplace=True)
    df.drop(df[df['itemid'] == 122319430122].index, inplace=True)
    df.drop(df[df['itemid'] == 322439374219].index, inplace=True)
    df.drop(df[df['itemid'] == 192121219541].index, inplace=True)
    df.drop(df[df['itemid'] == 222418277892].index, inplace=True)
    df.drop(df[df.endtime.dt.year <= 2016].index, inplace=True)
    df.drop(df[df.endtime.dt.month <= 3].index, inplace=True)
    logger.info("Number of records after dropping problems: {}".format(len(df)))

    # create a connection to write df to database
    engine = create_engine('postgresql://postgres:apassword@localhost:5432/postgres')

    # load the dataframe into the database
    df.to_sql(name='ebay_merged', con=engine, if_exists = 'replace', chunksize=2500, index=False)

if __name__ == "__main__":
    cli()
