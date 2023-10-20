import polars as pl
from datetime import datetime
from deltalake import DeltaTable
from deltalake import _internal

def load_data_full_refresh(df: pl.DataFrame) -> None:
    df.write_delta("deltalake/reject_layer/employees", 
                    mode="overwrite")

def load_data_incremental(df: pl.DataFrame, execution_date: datetime = datetime.now()) -> None:
    current_date_str = execution_date.strftime("%Y-%m-%d")
    DeltaTable("deltalake/reject_layer/employees").delete(f"join_date = '{current_date_str}'")
    df.write_delta("deltalake/reject_layer/employees", 
                    mode="append")
    
def load_data(df: pl.LazyFrame, execution_date: datetime = datetime.now(), is_full_refresh: bool = False) -> None:
    eager_df = df.collect()
    
    if is_full_refresh:
        load_data_full_refresh(eager_df)
    else:
        try:
            load_data_incremental(eager_df, execution_date)
        except _internal.TableNotFoundError:
            load_data_full_refresh(eager_df)

if __name__=='__main__': 
    pass