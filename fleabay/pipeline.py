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


@click.command()
def cli():

    """
    Pipeline to run data collection, ETL process, and merge data
    """

    # collect the API data
    collect_api.cli()
    logger.info("API data collection completed")

    # collect the web data
    collect_web.clii()
    logger.info("Web data collection completed")

    # etl the API data
    etl_api.cli()
    logger.info("API ETL completed")

    # etl the web data
    etl_web.cli()
    logger.info("Web ETL completed")

    # merge the API data and web data
    merge_data.cli()
    logger.info("API and web data merge completed")
