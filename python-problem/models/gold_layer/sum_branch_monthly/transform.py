import polars as pl

def pre_aggregation_transformation(df: pl.LazyFrame):
    enriched_df = df.with_columns(
                    ## work duration calculation
                    pl.when(pl.col("timesheet_checkout_datetime").is_null() | pl.col("timesheet_checkin_datetime").is_null())
                    .then(pl.duration(hours=8)) # if checkin or checkout is null assume its 8 hours
                    .otherwise(pl.col("timesheet_checkout_datetime").sub(pl.col("timesheet_checkin_datetime")))
                    .alias("total_work_duration"),
                    
                    ## flagging for recruited employee
                    pl.when(pl.col("timesheet_date").dt.truncate("1mo") == pl.col("employee_join_date").dt.truncate("1mo"))
                    .then(pl.col("employee_id"))
                    .otherwise(None)
                    .alias("recruited_employee_id"),
                    
                    ## flagging for resigned employee
                    pl.when(pl.col("timesheet_date").dt.truncate("1mo") == pl.col("employee_resignation_date").dt.truncate("1mo"))
                    .then(pl.col("employee_id"))
                    .otherwise(None)
                    .alias("resigned_employee_id")
                )
    
    return enriched_df

def transform_data(dfs: dict[str, pl.LazyFrame]):
    employees_df, timesheets_df = dfs['employees'], dfs['timesheets']
    
    timesheets_expanded_df = timesheets_df.join(employees_df, on="employee_id", how="inner")
    timesheets_expanded_df = pre_aggregation_transformation(timesheets_expanded_df)
    
    branch_monthly = timesheets_expanded_df.group_by(
                                                pl.col("timesheet_date").dt.truncate("1y").alias("year"), 
                                                pl.col("timesheet_date").dt.truncate("1mo").alias("month"), 
                                                "branch_id"
                                            ).agg(
                                                pl.col("total_work_duration").sum().dt.hours().alias("total_work_duration_hour"),
                                                pl.col("employee_monthly_salary_amount").mean().alias("average_monthly_salary_amount"),
                                                pl.col("employee_monthly_salary_amount").sum().alias("total_monthly_salary_amount"),
                                                pl.col("employee_id").n_unique().alias("total_employee_count"),
                                                pl.col("recruited_employee_id").drop_nulls().n_unique().fill_null(0).alias("total_recruited_employee_count"),
                                                pl.col("resigned_employee_id").drop_nulls().n_unique().fill_null(0).alias("total_resigned_employee_count")
                                            ).with_columns(
                                                pl.col("total_monthly_salary_amount").truediv(pl.col("total_work_duration_hour")).alias("salary_per_hour_amount")
                                            )

    return branch_monthly

if __name__=='__main__': 
    pass