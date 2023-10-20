import unittest
import polars as pl
from datetime import datetime
from models.reject_layer.employees.transform import seperate_duplicates, seperate_invalid_values, transform_data
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
        self.uncasted_rejected_schema = {
                "employe_id": pl.Utf8,
                "branch_id": pl.Utf8,
                "salary": pl.Utf8,
                "join_date": pl.Utf8,
                "resign_date": pl.Utf8,
                "rejected_reason": pl.Utf8
            }
        self.casted_schema = {
                "employe_id": pl.Int32, 
                "branch_id": pl.Int32, 
                "salary": pl.Int64, 
                "join_date": pl.Date, 
                "resign_date": pl.Date
            }

        self.bad_quality_df = pl.LazyFrame(
            {
                "employe_id": ["1", "1", "4", "abc", "7", "8", "9"],
                "branch_id": ["3", "2", "2", "6", "1", "1", "1"],
                "salary": ["7500000", "5500000", "1500000", "5500000", "7500000", "13000000", "13500000"],
                "join_date": ["2018-08-23", "2018-08-24", "2018-13-24", "2018-09-01", "2017-04-28", "2017-04-28", "2017-12-22"],
                "resign_date": [None, None, None, None, None, None, "2020-10-14"]
            }, schema=self.uncasted_schema
        )
        return super().setUp()

    def test_seperate_duplicates(self):
        expected_duplicate_df = pl.LazyFrame(
            {
                "employe_id": ["1", "1"],
                "branch_id": ["3", "2"],
                "salary": ["7500000", "5500000"],
                "join_date": ["2018-08-23", "2018-08-24"],
                "resign_date": [None, None]
            }, schema=self.uncasted_schema
        )
        expected_unique_df = pl.LazyFrame(
            {
                "employe_id": ["4", "abc", "7", "8", "9"],
                "branch_id": ["2", "6", "1", "1", "1"],
                "salary": ["1500000", "5500000", "7500000", "13000000", "13500000"],
                "join_date": ["2018-13-24", "2018-09-01", "2017-04-28", "2017-04-28", "2017-12-22"],
                "resign_date": [None, None, None, None, "2020-10-14"]
            }, schema=self.uncasted_schema
        )
        
        duplicate_df, unique_df = seperate_duplicates(self.bad_quality_df)
        
        assert_frame_equal(expected_unique_df, unique_df)
        assert_frame_equal(expected_duplicate_df, duplicate_df)

    def test_seperate_invalid_values(self):
        expected_uncasted_df = pl.LazyFrame(
            {
                "employe_id": ["4", "abc"],
                "branch_id": ["2", "6"],
                "salary": ["1500000", "5500000"],
                "join_date": ["2018-13-24", "2018-09-01"],
                "resign_date": [None, None]
            }, schema=self.uncasted_schema
        )
        expected_casted_df = pl.LazyFrame(
            {
                "employe_id": [7, 8, 9],
                "branch_id": [1, 1, 1],
                "salary": [7500000, 13000000, 13500000],
                "join_date": [datetime(2017,4,28), datetime(2017,4,28), datetime(2017,12,22)],
                "resign_date": [None, None, datetime(2020,10,14)]
            }, schema=self.casted_schema
        )
        
        _, unique_df = seperate_duplicates(self.bad_quality_df)
        uncasted_df, casted_df = seperate_invalid_values(unique_df)
        
        assert_frame_equal(expected_uncasted_df, uncasted_df)
        assert_frame_equal(expected_casted_df, casted_df)
        
    def test_transform_data(self):
        expected_duplicate_df = pl.LazyFrame(
            {
                "employe_id": ["1", "1"],
                "branch_id": ["3", "2"],
                "salary": ["7500000", "5500000"],
                "join_date": ["2018-08-23", "2018-08-24"],
                "resign_date": [None, None],
                "rejected_reason": ["DUPLICATE KEY DETECTED", "DUPLICATE KEY DETECTED"]
            }, schema=self.uncasted_rejected_schema
        )
        
        expected_uncasted_df = pl.LazyFrame(
            {
                "employe_id": ["4", "abc"],
                "branch_id": ["2", "6"],
                "salary": ["1500000", "5500000"],
                "join_date": ["2018-13-24", "2018-09-01"],
                "resign_date": [None, None],
                "rejected_reason": ["DATA TYPE MISMATCH", "DATA TYPE MISMATCH"]
            }, schema=self.uncasted_rejected_schema
        )
        
        expected_dirty_df = pl.concat([expected_duplicate_df, expected_uncasted_df], rechunk=True)
        dirty_df = transform_data(self.bad_quality_df)
        
        assert_frame_equal(expected_dirty_df, dirty_df)

if __name__ == '__main__':
    unittest.main()