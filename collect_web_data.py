#!/usr/bin/env python

import logging
import click
from ebaysdk.finding import Connection as Finding
from ebaysdk.exception import ConnectionError
import psycopg2
import json
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


def scrape_item_attr(cur, items):

    """
    iterate through the URLS, and write HTML to database
    """

    cnt = 0

    for line in items:
        response = urllib2.urlopen(line[1])
        data = {'scrape_date' : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'code' : response.code,
                'url'  : response.url,
                'read' : response.read(),
                'itemId' : int(line[0])}
    
        try:
            cur.execute("INSERT INTO ebay_web_raw (itemid, ad) VALUES (%s, %s)", [data['itemId'], json.dumps(data)])
            logger.info("Inserted new record: {}".format(line[0]))
            cnt += 1

        except:
            logger.info("Failed to insert new record: {}".format(line[0]))
            pass

        sleep(random() * 5.0)

    logger.info("Number of new records inserted: {}".format(cnt))


@click.command()
def cli():

    """
    Collect data from API and Web Scrapper
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

    # create a table for the web scapped data if it doesn't exists
    cur.execute("""CREATE TABLE IF NOT EXISTS ebay_web_raw
                   (id SERIAL PRIMARY KEY NOT NULL,
                    itemId bigint UNIQUE NOT NULL,
                    ad JSONB)""")

    # get a list of URLS to scrape
    cur.execute("""SELECT ad -> 'itemId', ad -> 'viewItemURL' 
                   FROM ebay_api_raw
                   WHERE itemId NOT IN (SELECT itemId FROM ebay_web_raw)""")
    items = [line for line in cur]
    logger.info("Number of ads to scrape for item details: {}".format(len(items)))

    # scrape website ads and store results into the databse
    logger.info("Starting collecting web data")    
    scrape_item_attr(cur, items) 
    logger.info("Completed collecting web data")

if __name__ == "__main__":
    cli()
