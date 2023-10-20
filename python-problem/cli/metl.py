import click

from cli import ingest as ingest_command
from cli import check as check_command

@click.group(help="Mini ETL CLI for Python-based Ingestion")
def metl():
    pass

metl.add_command(ingest_command.ingest)
metl.add_command(check_command.check)