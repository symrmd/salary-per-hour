import polars as pl

def seperate_duplicates(df: pl.LazyFrame):
    unique_df = df.unique(subset=["employe_id"], keep="none")
    duplicate_rows_df = df.join(unique_df, on="employe_id", how="anti")
    
    return duplicate_rows_df, unique_df

def seperate_invalid_values(df: pl.LazyFrame):
    # cast columns to expected dtypes
    casted_df = df.with_columns(
        pl.col("employe_id").cast(pl.Int32, strict=False).suffix("_casted"),
        pl.col("branch_id").cast(pl.Int32, strict=False).suffix("_casted"),
        pl.col("salary").cast(pl.Int64, strict=False).suffix("_casted"),
        pl.col("join_date").str.to_date("%Y-%m-%d", strict=False).suffix("_casted"),
        pl.col("resign_date").str.to_date("%Y-%m-%d", strict=False).suffix("_casted")
    )
    
    # flag cast-error records
    is_unable_to_cast_expression = (
        (pl.col("employe_id").is_null() != pl.col("employe_id_casted").is_null()) | 
        (pl.col("branch_id").is_null() != pl.col("branch_id_casted").is_null()) | 
        (pl.col("salary").is_null() != pl.col("salary_casted").is_null()) | 
        (pl.col("join_date").is_null() != pl.col("join_date_casted").is_null()) | 
        (pl.col("resign_date").is_null() != pl.col("resign_date_casted").is_null())
    )
    error_flagged_casted_df = casted_df.with_columns(
        is_unable_to_cast_expression.alias("is_unable_to_cast")
    )
    
    # seperate dirty data by flag
    uncasted_df = error_flagged_casted_df.filter(pl.col("is_unable_to_cast") == True) \
                                    .drop(["employe_id_casted", "branch_id_casted", "salary_casted", "join_date_casted", "resign_date_casted", "is_unable_to_cast"])
    casted_df = error_flagged_casted_df.filter(pl.col("is_unable_to_cast") == False) \
                                    .drop(["employe_id", "branch_id", "salary", "join_date", "resign_date", "is_unable_to_cast"]) \
                                    .rename({
                                            "employe_id_casted": "employe_id",
                                            "branch_id_casted": "branch_id",
                                            "salary_casted": "salary",
                                            "join_date_casted": "join_date",
                                            "resign_date_casted": "resign_date"
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
    pass