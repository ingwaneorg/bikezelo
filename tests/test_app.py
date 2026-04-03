import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, calculate_forecast


class TestCalculateForecast:
    def test_happy_path(self):
        df = pd.DataFrame({
            "row_id": [1, 2, 3],
            "timestamp": ["2026-01-01T10:00:00", "2026-01-01T10:01:00", "2026-01-01T10:02:00"],
        })
        result = calculate_forecast(df)
        assert result["rows_per_min"] > 0
        assert result["forecast_per_hour"] > 0

    def test_single_row_returns_zeros(self):
        df = pd.DataFrame({
            "row_id": [1],
            "timestamp": ["2026-01-01T10:00:00"],
        })
        result = calculate_forecast(df)
        assert result == {"rows_per_min": 0, "forecast_per_hour": 0}

    def test_all_null_timestamps_returns_zeros(self):
        df = pd.DataFrame({
            "row_id": [1, 2],
            "timestamp": [None, None],
        })
        result = calculate_forecast(df)
        assert result == {"rows_per_min": 0, "forecast_per_hour": 0}

    def test_timestamps_too_close_returns_zeros(self):
        # Two rows with identical timestamps — elapsed < 0.1 min
        df = pd.DataFrame({
            "row_id": [1, 2],
            "timestamp": ["2026-01-01T10:00:00", "2026-01-01T10:00:00"],
        })
        result = calculate_forecast(df)
        assert result == {"rows_per_min": 0, "forecast_per_hour": 0}


class TestGetRowsNaNSerialization:
    def setup_method(self):
        self.client = app.test_client()

    def test_nan_customer_id_serialized_as_null(self):
        df = pd.DataFrame({
            "row_id": [1],
            "timestamp": ["2026-01-01T10:00:00"],
            "customer_id": [None],
            "order_amount": [50.0],
            "status": ["NEW"],
        })
        with patch("app.read_db", return_value=df):
            response = self.client.get("/data/rows?after=0")
            data = response.get_json()
            assert data["rows"][0]["customer_id"] is None

    def test_nan_order_amount_serialized_as_null(self):
        df = pd.DataFrame({
            "row_id": [1],
            "timestamp": ["2026-01-01T10:00:00"],
            "customer_id": ["CUST0001"],
            "order_amount": [np.nan],
            "status": ["NEW"],
        })
        with patch("app.read_db", return_value=df):
            response = self.client.get("/data/rows?after=0")
            data = response.get_json()
            assert data["rows"][0]["order_amount"] is None

    def test_valid_row_returned_as_white(self):
        df = pd.DataFrame({
            "row_id": [1],
            "timestamp": ["2026-01-01T10:00:00"],
            "customer_id": ["CUST0001"],
            "order_amount": [99.99],
            "status": ["PAID"],
        })
        with patch("app.read_db", return_value=df):
            response = self.client.get("/data/rows?after=0")
            data = response.get_json()
            assert data["rows"][0]["status_class"] == "white"
            assert data["rows"][0]["row_id"] == 1
