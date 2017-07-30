#!/usr/bin/env python

import logging
import click
from ebaysdk.finding import Connection as Finding
from ebaysdk.exception import ConnectionError
import psycopg2
from bs4 import BeautifulSoup
import json
import collections
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
import urllib2
from time import sleep
from random import random
import re


# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def convert_html(raw_html):

    """
    converts a list of beautiful soup html objects to list of dicts
    """

    # create an empty list to hold converted data
    item_attr = []

    # iterate through raw html data
    for line in raw_html:
        soup = BeautifulSoup(line['read'], "lxml")
    
        attr = {}   

        attr['url'] =  line['url']
        attr['code'] = line['code']
        attr['scrape_date'] = line['scrape_date']
        attr['itemId'] = line['itemId']    
        
        table = soup.find('div', 'itemAttr')

        try:
            for label in table.select('th, td.attrLabels'):
                key = re.sub(r'\W+', '', label.text.strip())
                value = label.find_next_sibling().text.strip()
                attr[str(key)] = value.encode('utf-8')

            item_attr.append(attr)
        
        except:
            pass
  
    return item_attr


def convert_data_types(df):

    """
    convert data types where appropriate
    """

    df['Year'] = pd.to_numeric(df['Year'])
    df['itemId'] = pd.to_numeric(df['itemId'])
    df['Mileage'] = pd.to_numeric(df['Mileage'].str.replace(',', ''), errors='coerce')
    df['Trim'] = df['Trim'].replace(np.nan, "N/A")
    df['SubModel'] = df['SubModel'].replace(np.nan, "N/A")

    return df


def trans_type(row):

    """
    simplify transmission type
    """
    
    if type(row['Transmission']) == float:
        return 'other'
    
    if 'auto' in row['Transmission'].lower():
        return 'automatic'
    
    if 'man' in row['Transmission'].lower():
        return 'manual'
    
    else:
        return 'other'


def ext_color(row):

    """
    simplify exterior color
    """
    
    if type(row['ExteriorColor']) == float:
        return 'other'
    else:
        return row['ExteriorColor'].lower().replace(' ', '_')


def vehicle_title(row):
   
    """
    simplify vehicle title
    """
 
    if type(row['VehicleTitle']) == float:
        return 'other'
    
    if 'Clear' in row['VehicleTitle']:
        return 'clear'
    
    if 'Salvage' in row['VehicleTitle']:
        return 'salvage'
    
    if 'Rebuilt' in row['VehicleTitle']:
        return 'rebuilt'
    
    if 'Flood' in row['VehicleTitle']:
        return 'flood'
    
    else:
        'other'
    

def num_cyl(row):

    """
    simplify number of cylinders
    """

    
    if type(row['NumberofCylinders']) == float:
        return np.NAN
    
    if '4' in row['NumberofCylinders']:
        return '4'
    
    if '6' in row['NumberofCylinders']:
        return '6'
    
    if '8' in row['NumberofCylinders']:
        return '8'
    

def trim_level(row):

    """
    look for each key in the models dict, and if found, 
    return the value for that key for the column 'TRIM'
    """    

    # create an dict with all models and their rank
    models = {'Other': 0,
              'LE'   : 1,
              'LT'   : 2,
              'LS'   : 3,
              'RS'   : 4,
              'SS'   : 5,
              'ZL1'  : 6,
              'Zl1'  : 6,
              'zl1'  : 6,
              'ZL-1' : 6,
              'zl-1' : 6,
              'Z/28' : 7,
              'Z28'  : 7,
              'COPO' : 8,
              'Copo' : 8,
              'copo' : 8}

    labels = {0 : 'Other',
              1 : 'LE',
              2 : 'LT',
              3 : 'LS',
              4 : 'RS',
              5 : 'SS',
              6 : 'ZL1',
              7 : 'Z/28',
              8 : 'COPO'}

    # create an empty baseline
    models_iden = [0, ]
    
    # see if each model is in the the post
    for key in models.keys():
        
        # if it is, append the rank number for that model
        if key in row['Trim']:
            models_iden.append(models[key])
            
        if key in row['SubModel']:
            models_iden.append(models[key])
            
    # select the highest ranking model and return the label for that model
    max_model = max(models_iden)
    
    return labels[max_model]


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
    cur.execute("""SELECT ad FROM ebay_web_raw""")
    web_html = [record[0] for record in cur]
    logger.info("Number of records from web: {}".format(len(web_html)))

    # convert HTML to list of dicts
    web_data = convert_html(web_html)
    logger.info("HTML data converted")

    # convert to dataframe
    web_df = pd.DataFrame(web_data)
    logger.info("Data loaded into DataFrame")

    # convert data types where appropriate
    web_df = convert_data_types(web_df) 
    logger.info("Converted data types")

    # simplify transmission type
    web_df['TRANS'] = web_df.apply(lambda row: trans_type(row), axis=1)
    logger.info("Converted data types")

    # simplify exterior color
    web_df['EXT_COLOR'] = web_df.apply(lambda row: ext_color(row), axis=1)
    logger.info("Exterior color field normalized")
 
    # simplify vehicle title
    web_df['TITLE'] = web_df.apply(lambda row: vehicle_title(row), axis=1)
    logger.info("Vehicle title field normalized")

    # simplify number of cylinders
    web_df['CYL'] = web_df.apply(lambda row: num_cyl(row), axis=1)
    logger.info("Number of cylinders field normalized")

    # simplify trim level
    web_df['TRIM'] = web_df.apply(lambda row: trim_level(row), axis=1)    
    logger.info("Trim level field normalized")

    # create a connection to write df to database
    engine = create_engine('postgresql://postgres:apassword@localhost:5432/postgres')
    web_df.to_sql(name='ebay_web', con=engine, if_exists = 'replace', chunksize=2500, index=False)    
    logger.info("Data loaded into database")
