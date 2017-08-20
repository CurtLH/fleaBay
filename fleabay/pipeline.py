#!/usr/bin/env python

import logging
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
    Collect data from API and Web Scrapper
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
