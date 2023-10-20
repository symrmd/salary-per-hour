import polars as pl
import duckdb
from datetime import datetime
from deltalake import DeltaTable
from deltalake import _internal

def load_data_full_refresh(df: pl.DataFrame) -> None:
    # load to deltalake
    df.write_delta("deltalake/gold_layer/sum_branch_monthly", 
                    mode="overwrite")
    
    # load to duckdb
    with duckdb.connect('duckdb/gold_layer.db') as con:
        sum_branch_month_df = duckdb.arrow(df.to_arrow())
        res = con.execute("CREATE OR REPLACE TABLE sum_branch_month AS SELECT * FROM sum_branch_month_df")

def load_data_incremental(df: pl.DataFrame, execution_date: datetime = datetime.now()) -> None:
    current_date_month = execution_date.strftime("%Y-%m-01")
    
    # load to deltalake
    DeltaTable("deltalake/gold_layer/sum_branch_monthly").delete(f"month = CAST('{current_date_month}' AS DATE)")
    df.write_delta("deltalake/reject_layer/sum_branch_monthly", 
                    mode="append")
    
    # load to duckdb
    with duckdb.connect('duckdb/gold_layer.db') as con:
        sum_branch_month_df = duckdb.arrow(df.to_arrow())
        con.execute("CREATE TABLE IF NOT EXISTS sum_branch_month AS SELECT * FROM sum_branch_month_df")
        con.execute(f"DELETE FROM sum_branch_month WHERE month = '{current_date_month}'")
        con.execute("INSERT INTO sum_branch_month SELECT * FROM sum_branch_month_df")
    
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