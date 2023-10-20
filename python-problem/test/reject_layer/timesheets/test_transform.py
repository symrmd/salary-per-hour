import unittest
import polars as pl
from datetime import datetime, time
from models.reject_layer.timesheets.transform import seperate_duplicates, seperate_invalid_values, transform_data
from polars.testing import assert_frame_equal

class TestRejectLayerTransformer(unittest.TestCase):
    
    def setUp(self) -> None:
        self.uncasted_schema = {
                "timesheet_id": pl.Utf8,
                "employee_id": pl.Utf8,
                "date": pl.Utf8,
                "checkin": pl.Utf8,
                "checkout": pl.Utf8
            }
        self.uncasted_rejected_schema = {
                "timesheet_id": pl.Utf8,
                "employee_id": pl.Utf8,
                "date": pl.Utf8,
                "checkin": pl.Utf8,
                "checkout": pl.Utf8,
                "rejected_reason": pl.Utf8
            }
        self.casted_schema = {
                "timesheet_id": pl.Int32, 
                "employee_id": pl.Int32, 
                "date": pl.Date, 
                "checkin": pl.Time, 
                "checkout": pl.Time
            }

        self.bad_quality_df = pl.LazyFrame(
            {
                "timesheet_id": ["23907432", "23907432", "23907433", "abc", "23907435", "23907437", "23907443"],
                "employee_id": ["66", "66", "22", "21", "63", "60", "66"],
                "date": ["2019-08-21", "2019-08-21", "2019-08-21", "2019-08-21", "2019-08-21", "2019-08-21", "2019-08-22"],
                "checkin": ["08:13:31", "08:13:31", "08:56:34", "09:45:08", "25:55:47", "09:56:05", "08:28:27"],
                "checkout": ["17:05:02", "17:05:02", "18:00:48", "18:24:06", None, "17:31:08", "17:20:25"]
            }, schema=self.uncasted_schema
        )
        return super().setUp()

    def test_seperate_duplicates(self):
        expected_duplicate_df = pl.LazyFrame(
            {
                "timesheet_id": ["23907432", "23907432"],
                "employee_id": ["66", "66"],
                "date": ["2019-08-21", "2019-08-21"],
                "checkin": ["08:13:31", "08:13:31"],
                "checkout": ["17:05:02", "17:05:02"]
            }, schema=self.uncasted_schema
        )
        expected_unique_df = pl.LazyFrame(
            {
                "timesheet_id": ["23907433", "abc", "23907435", "23907437", "23907443"],
                "employee_id": ["22", "21", "63", "60", "66"],
                "date": ["2019-08-21", "2019-08-21", "2019-08-21", "2019-08-21", "2019-08-22"],
                "checkin": ["08:56:34", "09:45:08", "25:55:47", "09:56:05", "08:28:27"],
                "checkout": ["18:00:48", "18:24:06", None, "17:31:08", "17:20:25"]
            }, schema=self.uncasted_schema
        )
        
        duplicate_df, unique_df = seperate_duplicates(self.bad_quality_df)
        
        assert_frame_equal(expected_unique_df, unique_df)
        assert_frame_equal(expected_duplicate_df, duplicate_df)

    def test_seperate_invalid_values(self):
        expected_uncasted_df = pl.LazyFrame(
            {
                "timesheet_id": ["abc", "23907435"],
                "employee_id": ["21", "63"],
                "date": ["2019-08-21", "2019-08-21"],
                "checkin": ["09:45:08", "25:55:47"],
                "checkout": ["18:24:06", None]
            }, schema=self.uncasted_schema
        )
        expected_casted_df = pl.LazyFrame(
            {
                "timesheet_id": [23907433, 23907437, 23907443],
                "employee_id": [22, 60, 66],
                "date": [datetime(2019,8,21), datetime(2019,8,21), datetime(2019,8,22)],
                "checkin": [time(8, 56, 34), time(9, 56, 5), time(8, 28, 27)],
                "checkout": [time(18, 0, 48), time(17, 31, 8), time(17, 20, 25)]
            }, schema=self.casted_schema
        )
        
        _, unique_df = seperate_duplicates(self.bad_quality_df)
        uncasted_df, casted_df = seperate_invalid_values(unique_df)
        
        assert_frame_equal(expected_uncasted_df, uncasted_df)
        assert_frame_equal(expected_casted_df, casted_df)
        
    def test_transform_data(self):
        expected_duplicate_df = pl.LazyFrame(
            {
                "timesheet_id": ["23907432", "23907432"],
                "employee_id": ["66", "66"],
                "date": ["2019-08-21", "2019-08-21"],
                "checkin": ["08:13:31", "08:13:31"],
                "checkout": ["17:05:02", "17:05:02"],
                "rejected_reason": ["DUPLICATE KEY DETECTED", "DUPLICATE KEY DETECTED"]
            }, schema=self.uncasted_rejected_schema
        )
        
        expected_uncasted_df = pl.LazyFrame(
            {
                "timesheet_id": ["abc", "23907435"],
                "employee_id": ["21", "63"],
                "date": ["2019-08-21", "2019-08-21"],
                "checkin": ["09:45:08", "25:55:47"],
                "checkout": ["18:24:06", None],
                "rejected_reason": ["DATA TYPE MISMATCH", "DATA TYPE MISMATCH"]
            }, schema=self.uncasted_rejected_schema
        )
        
        expected_dirty_df = pl.concat([expected_duplicate_df, expected_uncasted_df], rechunk=True)
        dirty_df = transform_data(self.bad_quality_df)
        
        assert_frame_equal(expected_dirty_df, dirty_df)

if __name__ == '__main__':
    unittest.main()