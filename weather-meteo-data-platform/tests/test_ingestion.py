import unittest
from unittest.mock import MagicMock, Mock

from src.extractors.open_meteo import OpenMeteoExtractor
from src.loaders.postgres_loader import PostgresLoader


class ExtractorTest(unittest.TestCase):
    def setUp(self):
        self.extractor = OpenMeteoExtractor("https://example.invalid")
        self.addCleanup(self.extractor.session.close)

    def test_post_retries_temporary_status(self):
        policy = self.extractor.session.get_adapter(self.extractor.url).max_retries
        self.assertTrue(policy.is_retry("POST", 503))
        self.assertFalse(policy.is_retry("POST", 400))

    def test_rejects_invalid_hourly_payload(self):
        for payload in ([], [None], {"hourly": None}, {"hourly": {"time": []}},
                        {"hourly": {"time": ["2026-01-01T00:00"], "temperature": []}}):
            with self.subTest(payload=payload):
                self.extractor.session.post = Mock(
                    return_value=Mock(json=Mock(return_value=payload))
                )
                with self.assertRaises(RuntimeError):
                    self.extractor.get_open_meteo_data({}, {"hourly"})

    def test_allows_missing_measurements_as_null_values(self):
        payload = {"hourly": {"time": ["2026-01-01T00:00"], "temperature": [None]}}
        self.extractor.session.post = Mock(return_value=Mock(json=Mock(return_value=payload)))
        self.assertEqual(self.extractor.get_open_meteo_data({}, {"hourly"}), payload)


class BatchLoadTest(unittest.TestCase):
    def setUp(self):
        self.loader = PostgresLoader.__new__(PostgresLoader)
        self.loader.connection = Mock()
        self.loader.connection.closed = False
        self.cursor = Mock()
        self.loader.connection.cursor.return_value = MagicMock()
        self.loader.connection.cursor.return_value.__enter__.return_value = self.cursor

    def test_commits_both_sources_together(self):
        self.loader.load_forecast_batch("2026-01-01T00:00:00+00:00", [], [])
        self.assertEqual(self.cursor.execute.call_count, 2)
        self.loader.connection.commit.assert_called_once()

    def test_aq_write_failure_rolls_back_weather_too(self):
        self.cursor.execute.side_effect = [None, RuntimeError("simulated write failure")]
        with self.assertRaisesRegex(RuntimeError, "simulated write failure"):
            self.loader.load_forecast_batch("2026-01-01T00:00:00+00:00", [], [])
        self.loader.connection.commit.assert_not_called()
        self.loader.connection.rollback.assert_called_once()


if __name__ == "__main__":
    unittest.main()
