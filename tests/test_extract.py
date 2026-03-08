"""Tests for pipeline.extract — date utilities."""
from __future__ import annotations

from datetime import date

from pipeline.extract import yyyymmdd


class TestYyyymmdd:
    def test_standard_date(self):
        assert yyyymmdd(date(2015, 1, 1)) == "20150101"

    def test_pads_single_digit_month_and_day(self):
        assert yyyymmdd(date(2025, 3, 5)) == "20250305"

    def test_year_end(self):
        assert yyyymmdd(date(2024, 12, 31)) == "20241231"

    def test_leap_day(self):
        assert yyyymmdd(date(2024, 2, 29)) == "20240229"

    def test_october(self):
        assert yyyymmdd(date(2025, 10, 1)) == "20251001"

    def test_result_is_eight_chars(self):
        result = yyyymmdd(date(2020, 6, 15))
        assert len(result) == 8
        assert result.isdigit()
