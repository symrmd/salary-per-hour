import polars as pl

def transform_data(df: pl.LazyFrame):
    casted_df = df.with_columns(
        pl.col("employe_id").cast(pl.Int32, strict=False),
        pl.col("branch_id").cast(pl.Int32, strict=False),
        pl.col("salary").cast(pl.Int64, strict=False),
        pl.col("join_date").str.to_date("%Y-%m-%d", strict=False),
        pl.col("resign_date").str.to_date("%Y-%m-%d", strict=False)
    ).rename({
            "employe_id": "employee_id",
            "branch_id": "branch_id",
            "salary": "employee_monthly_salary_amount",
            "join_date": "employee_join_date",
            "resign_date": "employee_resignation_date"
        })
    
    return casted_df

if __name__=='__main__': 
    from extract import extract_data_full_refresh
    
    df = extract_data_full_refresh()
    transform_data(df)