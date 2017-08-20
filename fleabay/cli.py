import click
from fleabay import collect_api
from fleabay import collect_web
from fleabay import etl_api
from fleabay import etl_web
from fleabay import merge_data
from fleabay import pipeline


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
cli.add_command(merge_data.cli, 'merge_data')
cli.add_command(pipeline.cli, 'pipeline')
