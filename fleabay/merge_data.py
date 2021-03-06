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


def start_merge_data():

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
    logger.info("Number of records in API databse: {}".format(len(api_df)))
  
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
    logger.info("Number of records in web database: {}".format(len(web_df)))
    
    # reduce df to variables of interest
    web_df = web_df[['itemId', 'Mileage', 'Year', 'CYL', 'TITLE', 'TRANS', 'TRIM', 'EXT_COLOR']]

    # convert column names to lowercase
    web_df.columns = [x.lower() for x in web_df.columns]

    # merge api data and web data
    df = pd.merge(web_df, api_df, how='inner', on='itemid')
    logger.info("Number of records in merged dataset: {}".format(len(df)))

   # create a connection to write df to database
    engine = create_engine('postgresql://postgres:apassword@localhost:5432/postgres')

    # load the dataframe into the database
    df.to_sql(name='ebay_merged', con=engine, if_exists = 'replace', chunksize=2500, index=False)
    logger.info("Merged data written to database")



@click.command()
def cli():

    """
    Load API data and web data and merge into one dataset
    """

    start_merge_data()
