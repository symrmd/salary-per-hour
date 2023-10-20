import unittest
import polars as pl
from datetime import datetime
from models.silver_layer.employees.transform import transform_data
from polars.testing import assert_frame_equal

class TestRejectLayerTransformer(unittest.TestCase):
    
    def setUp(self) -> None:
        self.uncasted_schema = {
                "employe_id": pl.Utf8,
                "branch_id": pl.Utf8,
                "salary": pl.Utf8,
                "join_date": pl.Utf8,
                "resign_date": pl.Utf8
            }
        self.casted_schema = {
                "employee_id": pl.Int32, 
                "branch_id": pl.Int32, 
                "employee_monthly_salary_amount": pl.Int64, 
                "employee_join_date": pl.Date, 
                "employee_resignation_date": pl.Date
            }

        self.good_quality_df = pl.LazyFrame(
            {
                "employe_id": ["7", "8", "9"],
                "branch_id": ["1", "1", "1"],
                "salary": ["7500000", "13000000", "13500000"],
                "join_date": ["2017-04-28", "2017-04-28", "2017-12-22"],
                "resign_date": [None, None, "2020-10-14"]
            }, schema=self.uncasted_schema
        )
        return super().setUp()
        
    def test_transform_data(self):
        expected_cleaned_df = pl.LazyFrame(
            {
                "employee_id": [7, 8, 9],
                "branch_id": [1, 1, 1],
                "employee_monthly_salary_amount": [7500000, 13000000, 13500000],
                "employee_join_date": [datetime(2017,4,28), datetime(2017,4,28), datetime(2017,12,22)],
                "employee_resignation_date": [None, None, datetime(2020,10,14)]
            }, schema=self.casted_schema
        )
        
        cleaned_df = transform_data(self.good_quality_df)
        
        assert_frame_equal(expected_cleaned_df, cleaned_df)

if __name__ == '__main__':
    unittest.main()