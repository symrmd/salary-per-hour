import polars as pl

def transform_data(df: pl.LazyFrame) -> pl.LazyFrame:
    return df

if __name__=='__main__': 
    from extract import extract_data_full_refresh
    
    df = extract_data_full_refresh()
    transform_data(df)