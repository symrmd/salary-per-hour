import click, importlib
import polars as pl

@click.command(help="Check Command for Mini ETL App")
@click.option("-l", "--layer_name", required=True, help="Layer name, consist of bronze_layer, reject_layer, silver_layer, & gold_layer")
@click.option("-m", "--model_name", required=True, help="Model name, the name of the folder inside every layers' folder")
def check(layer_name, model_name):
    df = pl.read_delta(f"deltalake/{layer_name}/{model_name}")
    
    print(df.describe)