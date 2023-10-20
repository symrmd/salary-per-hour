import polars as pl

def handle_checkout_less_than_checkin(df: pl.LazyFrame):
    enriched_df = df.with_columns(
        pl.when(pl.col("checkin") > pl.col("checkout"))
        .then(pl.col("checkout") + pl.duration(days=1))
        .otherwise(pl.col("checkout")).alias("checkout")
    )
    
    return enriched_df

def transform_data(df: pl.LazyFrame):
    # cast all string columns
    enriched_df = df.with_columns(
        timesheet_id=pl.col("timesheet_id").cast(pl.Int32, strict=False),
        employee_id=pl.col("employee_id").cast(pl.Int32, strict=False),
        date=pl.col("date").str.to_date("%Y-%m-%d", strict=False),
        
        # time is not a supported dtype in delta table, so need to cast it to datetime
        checkin=pl.col("date").str.to_date("%Y-%m-%d", strict=False)
                    .dt.combine(pl.col("checkin").str.to_time("%H:%M:%S", strict=False)),
        checkout=pl.col("date").str.to_date("%Y-%m-%d", strict=False)
                    .dt.combine(pl.col("checkout").str.to_time("%H:%M:%S", strict=False)),
    )
    enriched_df = handle_checkout_less_than_checkin(enriched_df)
    
    # rename columns to more descriptive ones
    enriched_df = enriched_df.rename({
            "timesheet_id": "timesheet_id",
            "employee_id": "employee_id",
            "date": "timesheet_date",
            "checkin": "timesheet_checkin_datetime",
            "checkout": "timesheet_checkout_datetime"
        })
    
    return enriched_df

if __name__=='__main__': 
    from extract import extract_data_full_refresh
    
    df = extract_data_full_refresh()
    transform_data(df)