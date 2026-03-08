"""Tests for pipeline.run_daily — orchestration and date utilities."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from pipeline.run_daily import yesterday_utc_iso


class TestYesterdayUtcIso:
    def test_returns_string(self):
        result = yesterday_utc_iso()
        assert isinstance(result, str)

    def test_matches_iso_date_format(self):
        result = yesterday_utc_iso()
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", result), f"Not ISO date: {result!r}"

    def test_is_yesterday(self):
        expected = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        assert yesterday_utc_iso() == expected

    def test_result_is_parseable(self):
        from datetime import date

        result = yesterday_utc_iso()
        parsed = date.fromisoformat(result)
        assert parsed < datetime.now(timezone.utc).date()


class TestRunDailyMain:
    def test_main_runs_three_steps(self):
        """main() should invoke extract, transform, and compute sub-processes."""
        with patch("pipeline.run_daily.subprocess.run") as mock_run:
            mock_run.return_value = None
            from pipeline.run_daily import main

            main()
        assert mock_run.call_count == 3

    def test_main_runs_in_correct_order(self):
        """Steps must be: extract → transform → compute."""
        calls_made = []

        def capture_call(cmd, **kwargs):
            calls_made.append(cmd)

        with patch("pipeline.run_daily.subprocess.run", side_effect=capture_call):
            from pipeline.run_daily import main

            main()

        assert len(calls_made) == 3
        assert "pipeline.extract" in calls_made[0]
        assert "pipeline.transform" in calls_made[1]
        assert "pipeline.compute" in calls_made[2]

    def test_main_passes_date_to_extract(self):
        """extract step must receive --date with yesterday's date."""
        captured = []

        def capture_call(cmd, **kwargs):
            captured.append(cmd)

        expected_date = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        with patch("pipeline.run_daily.subprocess.run", side_effect=capture_call):
            from pipeline.run_daily import main

            main()

        extract_cmd = captured[0]
        assert "--date" in extract_cmd
        assert expected_date in extract_cmd

    def test_main_passes_date_to_compute(self):
        """compute step must receive --date with yesterday's date."""
        captured = []

        def capture_call(cmd, **kwargs):
            captured.append(cmd)

        expected_date = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        with patch("pipeline.run_daily.subprocess.run", side_effect=capture_call):
            from pipeline.run_daily import main

            main()

        compute_cmd = captured[2]
        assert "--date" in compute_cmd
        assert expected_date in compute_cmd

    def test_main_transform_has_no_date_arg(self):
        """transform step does not take a --date argument."""
        captured = []

        def capture_call(cmd, **kwargs):
            captured.append(cmd)

        with patch("pipeline.run_daily.subprocess.run", side_effect=capture_call):
            from pipeline.run_daily import main

            main()

        transform_cmd = captured[1]
        assert "--date" not in transform_cmd

    def test_main_uses_check_true(self):
        """subprocess.run must be called with check=True for error propagation."""
        with patch("pipeline.run_daily.subprocess.run") as mock_run:
            mock_run.return_value = None
            from pipeline.run_daily import main

            main()

        for c in mock_run.call_args_list:
            assert c.kwargs.get("check") is True, "subprocess.run must use check=True"
