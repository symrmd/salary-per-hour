import unittest
import polars as pl
from datetime import datetime, time
from models.gold_layer.sum_branch_monthly.transform import pre_aggregation_transformation, transform_data
from polars.testing import assert_frame_equal

class TestRejectLayerTransformer(unittest.TestCase):
    
    def setUp(self) -> None:
        self.joined_schema = {
                "timesheet_id": pl.Int32, 
                "employee_id": pl.Int32, 
                "timesheet_date": pl.Date, 
                "timesheet_checkin_datetime": pl.Datetime, 
                "timesheet_checkout_datetime": pl.Datetime,
                "branch_id": pl.Int32, 
                "employee_monthly_salary_amount": pl.Int64, 
                "employee_join_date": pl.Date, 
                "employee_resignation_date": pl.Date
            }
        
        self.preaggregated_schema = {
            "timesheet_id": pl.Int32, 
            "employee_id": pl.Int32, 
            "timesheet_date": pl.Date, 
            "timesheet_checkin_datetime": pl.Datetime, 
            "timesheet_checkout_datetime": pl.Datetime,
            "branch_id": pl.Int32, 
            "employee_monthly_salary_amount": pl.Int64, 
            "employee_join_date": pl.Date, 
            "employee_resignation_date": pl.Date,
            "total_work_duration": pl.Duration,
            "recruited_employee_id": pl.Int32,
            "resigned_employee_id": pl.Int32,
        }
        
        self.aggregated_schema = {
            "year": pl.Date, 
            "month": pl.Date, 
            "branch_id": pl.Int32, 
            "total_work_duration_hour": pl.Int32,
            "average_monthly_salary_amount": pl.Float64,
            "total_monthly_salary_amount": pl.Int64,
            "total_employee_count": pl.Int32,
            "total_recruited_employee_count": pl.Int32,
            "total_resigned_employee_count": pl.Int32,
            "salary_per_hour_amount": pl.Int32,
        }

        self.joined_df = pl.LazyFrame(
            {
                "timesheet_id": [23907433, 23907437, 23907443],
                "employee_id": [22, 60, 66],
                "timesheet_date": [datetime(2019,8,21), datetime(2019,8,21), datetime(2019,8,22)],
                "timesheet_checkin_datetime": [None, datetime(2019, 8, 21, 9, 56, 5), datetime(2019, 8, 22, 8, 28, 27)],
                "timesheet_checkout_datetime": [datetime(2019, 8, 21,18, 0, 48), None, datetime(2019, 8, 23, 6, 20, 25)],
                "branch_id": [1, 2, 3],
                "employee_monthly_salary_amount": [7500000, 13000000, 13500000],
                "employee_join_date": [datetime(2019,8,28), datetime(2017,4,28), datetime(2017,12,22)],
                "employee_resignation_date": [None, None, datetime(2019,8,14)]
            }, schema=self.joined_schema
        )
        return super().setUp()
    
    def test_pre_aggregation_transformation(self):
        expected_enriched_df = pl.LazyFrame(
            {
                "timesheet_id": [23907433, 23907437, 23907443],
                "employee_id": [22, 60, 66],
                "timesheet_date": [datetime(2019,8,21), datetime(2019,8,21), datetime(2019,8,22)],
                "timesheet_checkin_datetime": [None, datetime(2019, 8, 21, 9, 56, 5), datetime(2019, 8, 22, 8, 28, 27)],
                "timesheet_checkout_datetime": [datetime(2019, 8, 21,18, 0, 48), None, datetime(2019, 8, 23, 6, 20, 25)],
                "branch_id": [1, 2, 3],
                "employee_monthly_salary_amount": [7500000, 13000000, 13500000],
                "employee_join_date": [datetime(2019,8,28), datetime(2017,4,28), datetime(2017,12,22)],
                "employee_resignation_date": [None, None, datetime(2019,8,14)],
                "total_work_duration": [time(hour=8), time(hour=8), time(hour=21, minute=51, second=58)],
                "recruited_employee_id": [22, None, None],
                "resigned_employee_id": [None, None, 66]
            }, schema=self.preaggregated_schema
        )
        
        enriched_df = pre_aggregation_transformation(self.joined_df)
        
        assert_frame_equal(expected_enriched_df, enriched_df)

if __name__ == '__main__':
    unittest.main()