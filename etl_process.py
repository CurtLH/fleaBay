#!/usr/bin/env python

import logging
import click
from ebaysdk.finding import Connection as Finding
from ebaysdk.exception import ConnectionError
import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import urllib2
from time import sleep
from random import random

# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def flatten(d, parent_key='', sep='_'):

    """Thanks to Stackoverflow #6027558"""

    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    return dict(items)


# convert nested dict to unnested dict
def convert_api_dict(api_data):


    # flatten nested dicts
    api_data_flat = []
    for line in api_data:
        flat = flatten(line)
        api_data_flat.append(flat)

    # find out which columns store data in a list rather than a str
    list_cols = set()
    for line in api_data_flat:
        for key in line:
            if type(line[key]) == list:
                list_cols.add(key)

    # look through each dict and expand values that are stored as a list, and convert unicode to str
    data = []
    for line in api_data_flat:

        row = {}
        for key in line:
            if key == 'title':
                row[key] = line[key].encode('utf-8')

            elif key in list_cols and type(line[key]) == unicode:
                new_key = str(key) + "_" + str(line[key])
                new_value = 'true'
                row[new_key] = new_value

            elif key in list_cols and type(line[key]) == list:
                for item in line[key]:
                    new_key = str(key) + "_" + str(item)
                    new_value = 'true'
                    row[new_key] = new_value

            elif type(line[key]) == unicode:
                new_key = str(key)
                new_value = str(line[key])
                row[new_key] = new_value

        data.append(row)

    return data


def clean_up_api_df(df):

    # convert itemId to numberic
    df['itemId'] = pd.to_numeric(df['itemId'])

    # convert 'true'/'false' to bool
    df = df.replace('true', True)
    df = df.replace('false', False)

    # convert datetime values to datetime
    for col in df.columns:
        if col.endswith("startTime") or col.endswith("endTime"):
            df[col] = pd.to_datetime(df[col])

    # in columns where the only value is True, fill in missing values as False
    for col in df.columns:
        uniq_vals = df[col].unique().tolist()

        if uniq_vals == [np.nan, True] or uniq_vals == [True, np.nan]:
            df[col] = df[col].fillna(False)

        if uniq_vals == [np.nan, False] or uniq_vals == [False, np.nan]:
            df[col] = df[col].fillna(True)

    return df


##### MAIN PROGRAM #####
@click.command()
def cli():

    """
    Load raw data from the source database, clean data, load into target database
    """

    # connect to the databse
    try:
        conn = psycopg2.connect(database="postgres",
                                user="postgres",
                                password="apassword",
                                host="localhost")

        conn.autocommit = True
        cur = conn.cursor()
        logger.info("Successfully connected to the database")

    except:
        logger.info("Unable to connect to the database")


    # get all data from the eBay API
    cur.execute("""SELECT ad FROM ebay_api_raw""")
    api_data = [record[0] for record in cur]
    logger("Number of records from API: {}".format(len(api_data)))

    # convert nested dict to unnested dict
    data = convert_api_dict(api_data)

    # load data into df
    api_df = pd.DataFrame(data)

    # normalize the api data
    api_df= clean_up_api_df(df):

    # create a connection to write df to database
    engine = create_engine('postgresql://postgres:apassword@localhost:5432/postgres')
    api_df.to_sql(name='ebay_api', con=engine, if_exists = 'replace', chunksize=2500, index=False)    


if __name__ == "__main__":
    cli()
                                                     
