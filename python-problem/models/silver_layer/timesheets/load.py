import polars as pl
from datetime import datetime
from deltalake import DeltaTable
from deltalake import _internal

def load_data_full_refresh(df: pl.DataFrame) -> None:
    df.write_delta("deltalake/silver_layer/timesheets", 
                    mode="overwrite")

def load_data_incremental(df: pl.DataFrame, execution_date: datetime = datetime.now()) -> None:
    timesheets_arrow = df.to_arrow()
    timesheets_deltatable = DeltaTable("deltalake/silver_layer/timesheets")
    current_date_str = execution_date.strftime("%Y-%m-%d")
    
    merge_predicate = f"target.timesheet_id = source.timesheet_id and target.timesheet_date = CAST('{current_date_str}' AS DATE)"
    merge_table = timesheets_deltatable.merge(timesheets_arrow, predicate=merge_predicate, source_alias="source", target_alias="target")
    merge_table.when_matched_update_all() \
            .when_not_matched_insert_all() \
            .execute()
    
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