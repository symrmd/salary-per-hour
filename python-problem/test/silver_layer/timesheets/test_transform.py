import unittest
import polars as pl
from datetime import datetime
from models.silver_layer.timesheets.transform import transform_data, handle_checkout_less_than_checkin
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
        self.casted_schema = {
                "timesheet_id": pl.Int32, 
                "employee_id": pl.Int32, 
                "date": pl.Date, 
                "checkin": pl.Datetime, 
                "checkout": pl.Datetime
            }
        self.renamed_casted_schema = {
                "timesheet_id": pl.Int32, 
                "employee_id": pl.Int32, 
                "timesheet_date": pl.Date, 
                "timesheet_checkin_datetime": pl.Datetime, 
                "timesheet_checkout_datetime": pl.Datetime
            }

        self.good_quality_df = pl.LazyFrame(
            {
                "timesheet_id": ["23907433", "23907437", "23907443"],
                "employee_id": ["22", "60", "66"],
                "date": ["2019-08-21", "2019-08-21", "2019-08-22"],
                "checkin": ["08:56:34", "09:56:05", "08:28:27"],
                "checkout": ["18:00:48", "17:31:08", "06:20:25"]
            }, schema=self.uncasted_schema
        )
        return super().setUp()
    
    def test_handle_checkout_less_than_checkin(self):
        checkout_less_than_checkin_df = pl.LazyFrame(
            {
                "timesheet_id": [23907443],
                "employee_id": [66],
                "date": [datetime(2019,8,22)],
                "checkin": [datetime(2019, 8, 22, 8, 28, 27)],
                "checkout": [datetime(2019, 8, 22, 6, 20, 25)]
            }, schema=self.casted_schema
        )
        
        expected_fixed_df = pl.LazyFrame(
            {
                "timesheet_id": [23907443],
                "employee_id": [66],
                "date": [datetime(2019,8,22)],
                "checkin": [datetime(2019, 8, 22, 8, 28, 27)],
                "checkout": [datetime(2019, 8, 23, 6, 20, 25)]
            }, schema=self.casted_schema
        )
        
        fixed_df = handle_checkout_less_than_checkin(checkout_less_than_checkin_df)
        
        assert_frame_equal(expected_fixed_df, fixed_df)
        
    def test_transform_data(self):
        expected_cleaned_df = pl.LazyFrame(
            {
                "timesheet_id": [23907433, 23907437, 23907443],
                "employee_id": [22, 60, 66],
                "timesheet_date": [datetime(2019,8,21), datetime(2019,8,21), datetime(2019,8,22)],
                "timesheet_checkin_datetime": [datetime(2019, 8, 21, 8, 56, 34), datetime(2019, 8, 21, 9, 56, 5), datetime(2019, 8, 22, 8, 28, 27)],
                "timesheet_checkout_datetime": [datetime(2019, 8, 21,18, 0, 48), datetime(2019, 8, 21, 17, 31, 8), datetime(2019, 8, 23, 6, 20, 25)]
            }, schema=self.renamed_casted_schema
        )
        
        cleaned_df = transform_data(self.good_quality_df)
        
        assert_frame_equal(expected_cleaned_df, cleaned_df)

if __name__ == '__main__':
    unittest.main()