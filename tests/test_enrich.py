import asyncio
import unittest
from unittest.mock import patch

from app.enrich import EnrichRequest, EnrichResult, _ProxyDeadError, enrich_jobs


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
