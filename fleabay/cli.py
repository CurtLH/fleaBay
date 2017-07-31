import click
from fleabay import collect_api

@click.command()
def collect():

    collect_api.cli()
