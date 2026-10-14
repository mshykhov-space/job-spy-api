import asyncio
from concurrent.futures import ThreadPoolExecutor
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.enrich import (
    EnrichRequest,
    EnrichResult,
    _ProxyDeadError,
    _fetch_job_detail,
    _parse_retry_after,
    enrich_jobs,
)


class _FakeAsyncClient:
    def __init__(self, *, proxy: str, **_: object) -> None:
        self.proxy = proxy

    async def __aenter__(self) -> "_FakeAsyncClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None


class _FailingRequeueQueue(asyncio.Queue):
    def __init__(self) -> None:
        super().__init__()
        self.put_calls = 0

    async def put(self, item: object) -> None:
        self.put_calls += 1
        if self.put_calls > 1:
            raise RuntimeError("requeue failed")
        await super().put(item)


class EnrichJobsTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        from app import enrich

        with enrich._proxy_cooldown_lock:
            enrich._proxy_cooldowns.clear()

    async def test_requeued_job_is_processed_by_waiting_proxy(self) -> None:
        async def fetch_job(
            client: _FakeAsyncClient,
            url: str,
            job_id: str,
            _fingerprint: dict[str, str],
            _date_posted: str,
        ) -> EnrichResult:
            if client.proxy.endswith("proxy-one:8000"):
                await asyncio.sleep(0)
                raise _ProxyDeadError("rate_limited")
            return EnrichResult(job_id=job_id, url=url, status="success", data={"ok": True})

        request = EnrichRequest(
            jobs=[{"job_id": "42", "url": "https://example.com/jobs/42"}],
            proxies=[
                {"url": "user:pass@proxy-one:8000"},
                {"url": "user:pass@proxy-two:8000"},
            ],
            delay_min=0,
            delay_max=0,
        )

        with (
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            response = await asyncio.wait_for(enrich_jobs(request), timeout=1)

        self.assertEqual(response["stats"]["success"], 1)
        self.assertEqual(response["stats"]["proxies"]["alive"], 1)
        self.assertEqual(response["results"][0]["status"], "success")

    async def test_rate_limited_proxy_is_skipped_by_later_request(self) -> None:
        calls: list[str] = []

        async def fetch_job(
            client: _FakeAsyncClient,
            url: str,
            job_id: str,
            _fingerprint: dict[str, str],
            _date_posted: str,
        ) -> EnrichResult:
            calls.append(client.proxy)
            if client.proxy.endswith("proxy-one:8000"):
                raise _ProxyDeadError("rate_limited", retry_after_seconds=120)
            return EnrichResult(job_id=job_id, url=url, status="success", data={"ok": True})

        first = EnrichRequest(
            jobs=[{"job_id": "42", "url": "https://example.com/jobs/42"}],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )
        second = EnrichRequest(
            jobs=[{"job_id": "43", "url": "https://example.com/jobs/43"}],
            proxies=[
                {"url": "user:pass@proxy-one:8000"},
                {"url": "user:pass@proxy-two:8000"},
            ],
            delay_min=0,
            delay_max=0,
        )

        with (
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            first_response = await asyncio.wait_for(enrich_jobs(first), timeout=1)
            second_response = await asyncio.wait_for(enrich_jobs(second), timeout=1)

        self.assertEqual(first_response["results"][0]["status"], "skipped")
        self.assertEqual(first_response["stats"]["proxies"]["dead"]["proxy-one:8000"], "rate_limited")
        self.assertIn("proxy-one:8000", second_response["stats"]["proxies"]["cooling_down"])
        self.assertEqual(second_response["stats"]["success"], 1)
        self.assertEqual(calls.count("http://user:pass@proxy-one:8000"), 1)
        self.assertTrue(calls[-1].endswith("proxy-two:8000"))

    async def test_all_cooling_proxies_are_skipped_without_a_fetch(self) -> None:
        calls = 0

        async def fetch_job(*_: object) -> EnrichResult:
            nonlocal calls
            calls += 1
            raise _ProxyDeadError("rate_limited", retry_after_seconds=86400)

        request = EnrichRequest(
            jobs=[{"job_id": "42", "url": "https://example.com/jobs/42"}],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )
        with (
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            first_response = await asyncio.wait_for(enrich_jobs(request), timeout=1)
            second_response = await asyncio.wait_for(enrich_jobs(request), timeout=1)

        self.assertEqual(first_response["stats"]["proxies"]["cooling_down"]["proxy-one:8000"], 86400)
        self.assertEqual(second_response["results"][0]["status"], "skipped")
        self.assertEqual(second_response["stats"]["proxies"]["alive"], 0)
        self.assertEqual(calls, 1)

    async def test_missing_and_invalid_retry_after_escalate_after_expiration(self) -> None:
        from app import enrich

        outcomes = [None, None, None]
        calls = 0

        async def fetch_job(*args: object) -> EnrichResult:
            nonlocal calls
            retry_after = outcomes[calls]
            calls += 1
            raise _ProxyDeadError("rate_limited", retry_after_seconds=retry_after)

        request = EnrichRequest(
            jobs=[{"job_id": "42", "url": "https://example.com/jobs/42"}],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )
        with (
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            for expected_seconds in (900, 1800, 3600):
                response = await asyncio.wait_for(enrich_jobs(request), timeout=1)
                self.assertAlmostEqual(
                    response["stats"]["proxies"]["cooling_down"]["proxy-one:8000"],
                    expected_seconds,
                    delta=0.1,
                )
                with enrich._proxy_cooldown_lock:
                    enrich._proxy_cooldowns["proxy-one:8000"].deadline = enrich.time.monotonic() - 1

    async def test_success_resets_fallback_escalation(self) -> None:
        from app import enrich

        calls = 0

        async def fetch_job(*_: object) -> EnrichResult:
            nonlocal calls
            calls += 1
            if calls in (1, 2, 4):
                raise _ProxyDeadError("rate_limited")
            return EnrichResult(job_id="42", url="https://example.com/jobs/42", status="success")

        request = EnrichRequest(
            jobs=[{"job_id": "42", "url": "https://example.com/jobs/42"}],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )
        with (
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            first = await asyncio.wait_for(enrich_jobs(request), timeout=1)
            with enrich._proxy_cooldown_lock:
                enrich._proxy_cooldowns["proxy-one:8000"].deadline = enrich.time.monotonic() - 1
            second = await asyncio.wait_for(enrich_jobs(request), timeout=1)
            with enrich._proxy_cooldown_lock:
                enrich._proxy_cooldowns["proxy-one:8000"].deadline = enrich.time.monotonic() - 1
            success = await asyncio.wait_for(enrich_jobs(request), timeout=1)
            fourth = await asyncio.wait_for(enrich_jobs(request), timeout=1)

        self.assertEqual(first["stats"]["proxies"]["cooling_down"]["proxy-one:8000"], 900)
        self.assertEqual(second["stats"]["proxies"]["cooling_down"]["proxy-one:8000"], 1800)
        self.assertEqual(success["stats"]["success"], 1)
        self.assertNotIn("cooling_down", success["stats"]["proxies"])
        self.assertEqual(fourth["stats"]["proxies"]["cooling_down"]["proxy-one:8000"], 900)

    async def test_late_success_does_not_clear_newer_rate_limit(self) -> None:
        first_attempt_started = asyncio.Event()
        finish_first_attempt = asyncio.Event()
        calls: list[str] = []

        async def fetch_job(
            _client: _FakeAsyncClient,
            _url: str,
            job_id: str,
            _fingerprint: dict[str, str],
            _date_posted: str,
        ) -> EnrichResult:
            calls.append(job_id)
            if job_id == "42":
                first_attempt_started.set()
                await finish_first_attempt.wait()
                return EnrichResult(job_id=job_id, url="https://example.com/jobs/42", status="success")
            raise _ProxyDeadError("rate_limited")

        request = EnrichRequest(
            jobs=[{"job_id": "42", "url": "https://example.com/jobs/42"}],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )
        rate_limited_request = EnrichRequest(
            jobs=[{"job_id": "43", "url": "https://example.com/jobs/43"}],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )
        with (
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            late_success_task = asyncio.create_task(enrich_jobs(request))
            await asyncio.wait_for(first_attempt_started.wait(), timeout=1)
            rate_limited = await asyncio.wait_for(enrich_jobs(rate_limited_request), timeout=1)
            finish_first_attempt.set()
            late_success = await asyncio.wait_for(late_success_task, timeout=1)
            later_attempt = await asyncio.wait_for(enrich_jobs(request), timeout=1)

        self.assertEqual(rate_limited["results"][0]["status"], "skipped")
        self.assertEqual(late_success["stats"]["success"], 1)
        self.assertEqual(later_attempt["results"][0]["status"], "skipped")
        self.assertEqual(calls, ["42", "43"])

    async def test_expired_cooldown_allows_proxy_to_resume(self) -> None:
        from app import enrich

        calls = 0

        async def fetch_job(*args: object) -> EnrichResult:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise _ProxyDeadError("rate_limited", retry_after_seconds=120)
            return EnrichResult(job_id="42", url="https://example.com/jobs/42", status="success")

        request = EnrichRequest(
            jobs=[{"job_id": "42", "url": "https://example.com/jobs/42"}],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )
        with (
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            first_response = await asyncio.wait_for(enrich_jobs(request), timeout=1)
            with enrich._proxy_cooldown_lock:
                enrich._proxy_cooldowns["proxy-one:8000"].deadline = enrich.time.monotonic() - 1
            second_response = await asyncio.wait_for(enrich_jobs(request), timeout=1)

        self.assertEqual(first_response["results"][0]["status"], "skipped")
        self.assertEqual(second_response["stats"]["success"], 1)
        self.assertEqual(calls, 2)

    async def test_cancellation_does_not_leave_worker_or_queue_hanging(self) -> None:
        entered_fetch = asyncio.Event()
        release_fetch = asyncio.Event()

        async def fetch_job(*_: object) -> EnrichResult:
            entered_fetch.set()
            await release_fetch.wait()
            return EnrichResult(job_id="42", url="https://example.com/jobs/42", status="success")

        request = EnrichRequest(
            jobs=[{"job_id": "42", "url": "https://example.com/jobs/42"}],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )
        with (
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            task = asyncio.create_task(enrich_jobs(request))
            await asyncio.wait_for(entered_fetch.wait(), timeout=1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=1)

            release_fetch.set()
            response = await asyncio.wait_for(enrich_jobs(request), timeout=1)

        self.assertEqual(response["stats"]["success"], 1)

    async def test_worker_stops_fetching_after_overlapping_request_cools_proxy(self) -> None:
        first_job_started = asyncio.Event()
        continue_first_job = asyncio.Event()
        calls: list[str] = []

        async def fetch_job(
            _client: _FakeAsyncClient,
            _url: str,
            job_id: str,
            _fingerprint: dict[str, str],
            _date_posted: str,
        ) -> EnrichResult:
            calls.append(job_id)
            if job_id == "42":
                first_job_started.set()
                await continue_first_job.wait()
                return EnrichResult(job_id=job_id, url=f"https://example.com/jobs/{job_id}", status="success")
            if job_id == "43":
                raise _ProxyDeadError("rate_limited", retry_after_seconds=3600)
            return EnrichResult(job_id=job_id, url=f"https://example.com/jobs/{job_id}", status="success")

        first_request = EnrichRequest(
            jobs=[
                {"job_id": "42", "url": "https://example.com/jobs/42"},
                {"job_id": "44", "url": "https://example.com/jobs/44"},
            ],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )
        second_request = EnrichRequest(
            jobs=[{"job_id": "43", "url": "https://example.com/jobs/43"}],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )
        with (
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            first_task = asyncio.create_task(enrich_jobs(first_request))
            await asyncio.wait_for(first_job_started.wait(), timeout=1)
            second_response = await asyncio.wait_for(enrich_jobs(second_request), timeout=1)
            continue_first_job.set()
            first_response = await asyncio.wait_for(first_task, timeout=1)

        self.assertEqual(second_response["results"][0]["status"], "skipped")
        self.assertNotIn("44", calls)
        self.assertEqual({result["status"] for result in first_response["results"]}, {"success", "skipped"})

    def test_concurrent_cooldown_updates_keep_the_latest_deadline(self) -> None:
        from app import enrich

        enrich._record_proxy_rate_limit("proxy-one:8000", 3600)
        with ThreadPoolExecutor(max_workers=2) as executor:
            updates = [
                executor.submit(enrich._record_proxy_rate_limit, "proxy-one:8000", 120),
                executor.submit(enrich._record_proxy_rate_limit, "proxy-one:8000", 180),
            ]
            for update in updates:
                update.result()

        remaining = enrich._proxy_cooldown_snapshot(
            [enrich.ProxyConfig(url="user:pass@proxy-one:8000")]
        )["proxy-one:8000"]
        self.assertGreater(remaining, 3590)

    def test_retry_after_supports_seconds_and_http_date(self) -> None:
        now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(_parse_retry_after("120", now=now), 120)
        self.assertEqual(_parse_retry_after("Wed, 14 Oct 2026 12:02:00 GMT", now=now), 120)
        self.assertEqual(_parse_retry_after("Wed, 14 Oct 2026 11:59:00 GMT", now=now), 0)
        self.assertIsNone(_parse_retry_after("invalid", now=now))
        self.assertIsNone(_parse_retry_after(None, now=now))
        self.assertEqual(_parse_retry_after("3600", now=now), 3600)
        self.assertEqual(_parse_retry_after("86400", now=now), 86400)

    async def test_429_response_header_is_passed_into_proxy_cooldown(self) -> None:
        class _Response:
            status_code = 429
            headers = {"retry-after": "120"}

        class _Client:
            async def get(self, *_: object, **__: object) -> _Response:
                return _Response()

        with self.assertRaises(_ProxyDeadError) as raised:
            await _fetch_job_detail(
                _Client(),
                "https://example.com/jobs/42",
                "42",
                {},
            )

        self.assertEqual(raised.exception.reason, "rate_limited")
        self.assertEqual(raised.exception.retry_after_seconds, 120)

    async def test_429_without_or_with_invalid_header_uses_fallback(self) -> None:
        class _Response:
            status_code = 429

            def __init__(self, headers: dict[str, str]):
                self.headers = headers

        class _Client:
            def __init__(self, headers: dict[str, str]):
                self.headers = headers

            async def get(self, *_: object, **__: object) -> _Response:
                return _Response(self.headers)

        for headers in ({}, {"retry-after": "not a date"}):
            with self.assertRaises(_ProxyDeadError) as raised:
                await _fetch_job_detail(
                    _Client(headers),
                    "https://example.com/jobs/42",
                    "42",
                    {},
                )
            self.assertIsNone(raised.exception.retry_after_seconds)

    async def test_all_dead_proxies_return_skipped_jobs_without_hanging(self) -> None:
        async def fetch_job(*_: object) -> EnrichResult:
            raise _ProxyDeadError("proxy_error")

        request = EnrichRequest(
            jobs=[
                {"job_id": "42", "url": "https://example.com/jobs/42"},
                {"job_id": "43", "url": "https://example.com/jobs/43"},
            ],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )

        with (
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            response = await asyncio.wait_for(enrich_jobs(request), timeout=1)

        self.assertEqual(response["stats"]["success"], 0)
        self.assertEqual(response["stats"]["proxies"]["alive"], 0)
        self.assertEqual(len(response["results"]), 2)
        self.assertEqual({result["status"] for result in response["results"]}, {"skipped"})

    async def test_worker_error_wins_over_simultaneous_queue_completion(self) -> None:
        async def fetch_job(*_: object) -> EnrichResult:
            raise _ProxyDeadError("proxy_error")

        request = EnrichRequest(
            jobs=[{"job_id": "42", "url": "https://example.com/jobs/42"}],
            proxies=[{"url": "user:pass@proxy-one:8000"}],
            delay_min=0,
            delay_max=0,
        )

        with (
            patch("app.enrich.asyncio.Queue", _FailingRequeueQueue),
            patch("app.enrich.httpx.AsyncClient", _FakeAsyncClient),
            patch("app.enrich._fetch_job_detail", fetch_job),
        ):
            with self.assertRaisesRegex(RuntimeError, "requeue failed"):
                await asyncio.wait_for(enrich_jobs(request), timeout=1)


if __name__ == "__main__":
    unittest.main()
