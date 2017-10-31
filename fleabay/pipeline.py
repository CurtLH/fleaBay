#!/usr/bin/env python

import logging
import click
from fleabay import collect_api
from fleabay import collect_web
from fleabay import etl_api
from fleabay import etl_web
from fleabay import merge_data


# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def start_pipeline():

    """
    Collect data, run ETL process, and merge data
    """

    # collect API data
    collect_api.start_collect_api()
    logger.info("API data collection completed")

    # collect web data
    collect_web.start_collect_web()
    logger.info("Web data collection completed")

    # etl API data
    etl_api.start_etl_api()
    logger.info("API data ETL completed")

    # etl web data
    etl_web.start_etl_web()
    logger.info("Web data ETL completed")

    # merge API data and web data
    merge_data.start_merge_data()
    logger.info("API data and wbe data merge completed")

@click.command()
def cli():

    """
    Collect data, run ETL process, and merge data
    """

    start_pipeline()
