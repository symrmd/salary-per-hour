import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta

def extract_data_full_refresh() -> dict[str, pl.LazyFrame]:
    dwh_dfs = {
        'employees': pl.scan_delta("deltalake/silver_layer/employees"),
        'timesheets': pl.scan_delta("deltalake/silver_layer/timesheets")
    }
    
    return dwh_dfs

def extract_data_incremental(execution_date: datetime):
    current_month_str = execution_date.replace(day=1)
    next_month_str = (execution_date + relativedelta(months=+1)).replace(day=1)
    
    dwh_dfs = {
        'employees': pl.scan_delta("deltalake/silver_layer/employees").filter((pl.col("employee_join_date") < next_month_str)),
        'timesheets': pl.scan_delta("deltalake/silver_layer/timesheets").filter((pl.col("timesheet_date") >= current_month_str) & (pl.col("timesheet_date") < next_month_str))
    }

    return dwh_dfs

def extract_data(execution_date: datetime = datetime.now(), is_full_refresh: bool = False):
    if is_full_refresh:
        dwh_dfs = extract_data_full_refresh()
    else:
        dwh_dfs = extract_data_incremental(execution_date=execution_date)
        
    return dwh_dfs

if __name__=='__main__': 
    pass