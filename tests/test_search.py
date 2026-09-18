import logging
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


class _FakeFrame:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows

    def notna(self) -> "_FakeFrame":
        return self

    def where(self, _: object, __: object) -> "_FakeFrame":
        return self

    def to_dict(self, *, orient: str) -> list[dict[str, object]]:
        if orient != "records":
            raise AssertionError(f"unexpected orientation: {orient}")
        return self.rows


class _RecordingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


class StrictSearchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)
        self.request = {
            "site": "linkedin",
            "search_term": "Kotlin",
            "location": "Europe",
            "proxies": ["http://user:super-secret@proxy.example:8000"],
            "is_remote": True,
            "results_wanted": 100,
            "offset": 0,
            "hours_old": 24,
        }

    def test_error_log_turns_partial_results_into_sanitized_rate_limit(self) -> None:
        recording_handler = _RecordingHandler()
        jobspy_logger = logging.getLogger("JobSpy:LinkedIn")
        jobspy_logger.addHandler(recording_handler)

        def partial_result(**_: object) -> _FakeFrame:
            jobspy_logger.error(
                "429 Response through user:super-secret@proxy.example:8000"
            )
            return _FakeFrame([{"job_url": "https://example.com/jobs/1"}])

        try:
            with patch("app.main.scrape_jobs", partial_result):
                response = self.client.post("/jobs/search", json=self.request)
        finally:
            jobspy_logger.removeHandler(recording_handler)

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.json()["detail"]["code"], "upstream_rate_limited")
        self.assertNotIn("super-secret", response.text)
        self.assertEqual(recording_handler.messages, [])

    def test_normal_partial_page_remains_a_successful_array(self) -> None:
        captured: dict[str, object] = {}

        def partial_result(**kwargs: object) -> _FakeFrame:
            captured.update(kwargs)
            return _FakeFrame([{"job_url": "https://example.com/jobs/1"}])

        with patch("app.main.scrape_jobs", partial_result):
            response = self.client.post("/jobs/search", json=self.request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [{"job_url": "https://example.com/jobs/1"}])
        self.assertEqual(captured["offset"], 0)
        self.assertEqual(captured["results_wanted"], 100)
        self.assertEqual(captured["proxies"], ["user:super-secret@proxy.example:8000"])

    def test_concurrent_searches_do_not_share_failure_state(self) -> None:
        failing_started = threading.Event()
        release_failing = threading.Event()

        def search(**kwargs: object) -> _FakeFrame:
            if kwargs["search_term"] == "failing":
                failing_started.set()
                self.assertTrue(release_failing.wait(timeout=2))
                logging.getLogger("JobSpy:LinkedIn").error(
                    "LinkedIn transport failed with super-secret"
                )
                return _FakeFrame([{"job_url": "https://example.com/jobs/incomplete"}])
            return _FakeFrame([{"job_url": "https://example.com/jobs/complete"}])

        failing_request = self.request | {"search_term": "failing"}
        successful_request = self.request | {"search_term": "successful"}
        with (
            patch("app.main.scrape_jobs", search),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            failing_future = executor.submit(
                self.client.post, "/jobs/search", json=failing_request
            )
            self.assertTrue(failing_started.wait(timeout=2))
            successful_future = executor.submit(
                self.client.post, "/jobs/search", json=successful_request
            )
            release_failing.set()
            failing_response = failing_future.result(timeout=2)
            successful_response = successful_future.result(timeout=2)

        self.assertEqual(failing_response.status_code, 502)
        self.assertEqual(failing_response.json()["detail"]["code"], "upstream_failure")
        self.assertNotIn("super-secret", failing_response.text)
        self.assertEqual(successful_response.status_code, 200)
        self.assertEqual(
            successful_response.json(),
            [{"job_url": "https://example.com/jobs/complete"}],
        )

    def test_get_jobs_keeps_the_array_response_contract(self) -> None:
        with patch(
            "app.main.scrape_jobs",
            return_value=_FakeFrame([{"job_url": "https://example.com/jobs/1"}]),
        ):
            response = self.client.get(
                "/jobs", params={"site": "linkedin", "search_term": "Kotlin"}
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [{"job_url": "https://example.com/jobs/1"}])


if __name__ == "__main__":
    unittest.main()
