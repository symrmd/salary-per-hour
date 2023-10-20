import click, importlib
import iso8601

@click.command(help="Ingestion Command for Mini ETL App")
@click.option("-l", "--layer_name", required=True, help="Layer name, consist of bronze_layer, reject_layer, silver_layer, & gold_layer")
@click.option("-m", "--model_name", required=True, help="Model name, the name of the folder inside every layers' folder")
@click.option("-st", "--start_time", required=False, default="2000-01-01", help="Start Date or Timestamp, Use ISO Format")
@click.option("-f", "--full_refresh", is_flag=True, default=False, help="Whether or not the ingestion is full refresh or incremental")
def ingest(layer_name, model_name, start_time, full_refresh):
    execution_datetime = iso8601.parse_date(start_time)
    f_extract_data = getattr(importlib.import_module(f"models.{layer_name}.{model_name}.extract"), "extract_data")
    f_transform_data = getattr(importlib.import_module(f"models.{layer_name}.{model_name}.transform"), "transform_data")
    f_load_data = getattr(importlib.import_module(f"models.{layer_name}.{model_name}.load"), "load_data")
    
    extracted_data = f_extract_data(execution_datetime, full_refresh)
    transformed_data = f_transform_data(extracted_data)
    f_load_data(transformed_data, execution_datetime, full_refresh)