import click
from fleabay import collect_api
from fleabay import collect_web
from fleabay import etl_api
from fleabay import etl_web

@click.group()
def cli():

    """
    fleaBay is a data collector and ETL process
    """

    pass


cli.add_command(collect_api.cli, 'collect_api')
cli.add_command(collect_web.cli, 'collect_web')
cli.add_command(etl_api.cli, 'etl_api')
cli.add_command(etl_web.cli, 'etl_web')
