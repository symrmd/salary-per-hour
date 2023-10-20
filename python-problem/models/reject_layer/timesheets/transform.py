import polars as pl

def seperate_duplicates(df: pl.LazyFrame):
    unique_df = df.unique(subset=["timesheet_id"], keep="none")
    duplicate_rows_df = df.join(unique_df, on="timesheet_id", how="anti")
    
    return duplicate_rows_df, unique_df

def seperate_invalid_values(df: pl.LazyFrame):
    # cast columns to expected dtypes
    casted_df = df.with_columns(
        pl.col("timesheet_id").cast(pl.Int32, strict=False).suffix("_casted"),
        pl.col("employee_id").cast(pl.Int32, strict=False).suffix("_casted"),
        pl.col("date").str.to_date("%Y-%m-%d", strict=False).suffix("_casted"),
        pl.col("checkin").str.to_time("%H:%M:%S", strict=False).suffix("_casted"),
        pl.col("checkout").str.to_time("%H:%M:%S", strict=False).suffix("_casted"),
    )
    
    # flag cast-error records
    is_unable_to_cast_expression = (
        (pl.col("timesheet_id").is_null() != pl.col("timesheet_id_casted").is_null()) | 
        (pl.col("employee_id").is_null() != pl.col("employee_id_casted").is_null()) | 
        (pl.col("date").is_null() != pl.col("date_casted").is_null()) | 
        (pl.col("checkin").is_null() != pl.col("checkin_casted").is_null()) | 
        (pl.col("checkout").is_null() != pl.col("checkout_casted").is_null())
    )
    error_flagged_casted_df = casted_df.with_columns(
        is_unable_to_cast_expression.alias("is_unable_to_cast")
    )
    
    # seperate dirty data by flag
    uncasted_df = error_flagged_casted_df.filter(pl.col("is_unable_to_cast") == True) \
                                    .drop(["timesheet_id_casted", "employee_id_casted", "date_casted", "checkin_casted", "checkout_casted", "is_unable_to_cast"])
    casted_df = error_flagged_casted_df.filter(pl.col("is_unable_to_cast") == False) \
                                    .drop(["timesheet_id", "employee_id", "date", "checkin", "checkout", "is_unable_to_cast"]) \
                                    .rename({
                                            "timesheet_id_casted": "timesheet_id",
                                            "employee_id_casted": "employee_id",
                                            "date_casted": "date",
                                            "checkin_casted": "checkin",
                                            "checkout_casted": "checkout"
                                        })
    
    return uncasted_df, casted_df

def transform_data(df: pl.LazyFrame):
    duplicate_df, unique_df = seperate_duplicates(df)
    uncasted_df, _ = seperate_invalid_values(unique_df)
    
    duplicate_df = duplicate_df.with_columns(
        pl.lit("DUPLICATE KEY DETECTED").alias("rejected_reason")
    )
    uncasted_df = uncasted_df.with_columns(
        pl.lit("DATA TYPE MISMATCH").alias("rejected_reason")
    )
    
    return pl.concat([duplicate_df, uncasted_df], rechunk=True)

if __name__=='__main__': 
    from extract import extract_data_full_refresh
    
    df = extract_data_full_refresh()
    transform_data(df)