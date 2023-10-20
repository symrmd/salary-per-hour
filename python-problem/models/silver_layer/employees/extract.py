import polars as pl
from datetime import datetime

def extract_data_full_refresh():
    bronze_df = pl.scan_delta("deltalake/bronze_layer/employees")
    reject_df = pl.scan_delta("deltalake/reject_layer/employees")

    clean_df = bronze_df.join(reject_df, on="employe_id", how="anti")
    return clean_df

def extract_data_incremental(execution_date: datetime.date):
    current_date_str = execution_date.strftime("%Y-%m-%d")
    bronze_df = pl.scan_delta("deltalake/bronze_layer/employees").filter(pl.col("join_date") == current_date_str)
    reject_df = pl.scan_delta("deltalake/reject_layer/employees").filter(pl.col("join_date") == current_date_str)

    clean_df = bronze_df.join(reject_df, on="employe_id", how="anti")
    return clean_df

def extract_data(execution_date: datetime = datetime.now(), is_full_refresh: bool = False):
    if is_full_refresh:
        df = extract_data_full_refresh()
    else:
        df = extract_data_incremental(execution_date=execution_date)
        
    return df

if __name__=='__main__': 
    pass