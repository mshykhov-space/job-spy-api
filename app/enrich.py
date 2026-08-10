import asyncio
import logging
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from markdownify import markdownify as md

logger = logging.getLogger(__name__)

LINKEDIN_JOB_URL = "https://www.linkedin.com/jobs/view/{job_id}"
REQUEST_TIMEOUT = 30
_POSTED_TIME_RE = re.compile(r"Posted\s+(\d{1,2}:\d{2}:\d{2} [AP]M)")


@dataclass
class ProxyConfig:
    url: str
    fingerprint: dict[str, str] = field(default_factory=dict)

    @property
    def proxy_url(self) -> str:
        raw = self.url.removeprefix("http://").removeprefix("https://")
        return f"http://{raw}"

    @property
    def host(self) -> str:
        raw = self.url.removeprefix("http://").removeprefix("https://")
        return raw.split("@")[-1] if "@" in raw else raw


@dataclass
class EnrichRequest:
    jobs: list[dict]
    proxies: list[dict]
    delay_min: int = 7
    delay_max: int = 15


@dataclass
class EnrichResult:
    job_id: str
    url: str
    status: str
    data: dict | None = None
    error: str | None = None


@dataclass
class EnrichStats:
    total: int = 0
    success: int = 0
    failed: dict[str, int] = field(default_factory=dict)
    proxies_total: int = 0
    proxies_alive: int = 0
    proxy_deaths: dict[str, str] = field(default_factory=dict)
    duration_seconds: float = 0

    def to_dict(self) -> dict:
        d: dict = {
            "total": self.total,
            "success": self.success,
        }
        if self.failed:
            d["failed"] = self.failed
        proxies: dict = {"alive": self.proxies_alive}
        if self.proxy_deaths:
            proxies["dead"] = self.proxy_deaths
        d["proxies"] = proxies
        d["duration_seconds"] = self.duration_seconds
        return d


async def enrich_jobs(request: EnrichRequest) -> dict:
    start_time = time.monotonic()

    proxies = [ProxyConfig(url=p["url"], fingerprint=p.get("fingerprint", {})) for p in request.proxies]
    if not proxies:
        return _empty_response(request.jobs, "No proxies provided")

    queue: asyncio.Queue[dict] = asyncio.Queue()
    for job in request.jobs:
        await queue.put(job)

    results: list[EnrichResult] = []
    results_lock = asyncio.Lock()
    dead_proxies: dict[str, str] = {}

    async def worker(proxy_idx: int, proxy: ProxyConfig):
        async with httpx.AsyncClient(
            proxy=proxy.proxy_url,
            timeout=REQUEST_TIMEOUT,
            follow_redirects=False,
        ) as client:
            while True:
                job = await queue.get()

                job_id = job.get("job_id", "")
                url = job.get("url", LINKEDIN_JOB_URL.format(job_id=job_id))

                date_posted = job.get("date_posted", "")

                try:
                    result = await _fetch_job_detail(client, url, job_id, proxy.fingerprint, date_posted)
                    async with results_lock:
                        results.append(result)

                    if result.status == "success":
                        delay = random.uniform(request.delay_min, request.delay_max)
                        await asyncio.sleep(delay)
                    else:
                        await asyncio.sleep(2)

                except _ProxyDeadError as e:
                    logger.warning("Proxy %s dead: %s. Re-queuing job %s", proxy.host, e, job_id)
                    await queue.put(job)
                    dead_proxies[proxy.host] = e.reason
                    return

                except Exception as e:
                    logger.error("Unexpected error for job %s on proxy %d: %s", job_id, proxy_idx, e)
                    async with results_lock:
                        results.append(EnrichResult(job_id=job_id, url=url, status="error", error=str(e)))

                finally:
                    queue.task_done()

    worker_tasks = {asyncio.create_task(worker(i, p)) for i, p in enumerate(proxies)}
    pending_workers = set(worker_tasks)
    queue_done = asyncio.create_task(queue.join())

    try:
        while pending_workers:
            done, _ = await asyncio.wait(
                pending_workers | {queue_done},
                return_when=asyncio.FIRST_COMPLETED,
            )
            finished_workers = done & pending_workers
            for task in finished_workers:
                task.result()
            pending_workers -= finished_workers

            if queue_done in done:
                break
    finally:
        if not queue_done.done():
            queue_done.cancel()
        for task in pending_workers:
            task.cancel()
        await asyncio.gather(queue_done, *worker_tasks, return_exceptions=True)

    skipped_jobs = []
    while not queue.empty():
        job = queue.get_nowait()
        skipped_jobs.append(
            EnrichResult(
                job_id=job.get("job_id", ""),
                url=job.get("url", ""),
                status="skipped",
                error="All proxies exhausted",
            )
        )
    results.extend(skipped_jobs)

    stats = _compute_stats(results, len(proxies), dead_proxies, time.monotonic() - start_time)

    return {
        "results": [_result_to_dict(r) for r in results],
        "stats": stats.to_dict(),
    }


class _ProxyDeadError(Exception):
    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}" if detail else reason)


async def _fetch_job_detail(
    client: httpx.AsyncClient,
    url: str,
    job_id: str,
    fingerprint: dict[str, str],
    date_posted: str = "",
) -> EnrichResult:
    headers = _build_headers(fingerprint)

    try:
        response = await client.get(url, headers=headers)
    except httpx.TimeoutException:
        return EnrichResult(job_id=job_id, url=url, status="timeout", error="Request timed out")
    except httpx.ProxyError as e:
        raise _ProxyDeadError("proxy_error", str(e))
    except httpx.ConnectError as e:
        raise _ProxyDeadError("connect_error", str(e))

    if response.status_code == 429:
        raise _ProxyDeadError("rate_limited")

    if response.status_code in (301, 302, 303, 307, 308):
        location = response.headers.get("location", "")
        if "signup" in location or "login" in location or "authwall" in location:
            raise _ProxyDeadError("auth_wall", location)

    if response.status_code == 404:
        return EnrichResult(job_id=job_id, url=url, status="not_found", error="Job not found")

    if response.status_code >= 400:
        return EnrichResult(job_id=job_id, url=url, status="error", error=f"HTTP {response.status_code}")

    data = _parse_job_page(response.text)
    if not data:
        return EnrichResult(job_id=job_id, url=url, status="no_data", error="Could not parse job page")

    posted_time_exact = data.pop("posted_time_exact", None)
    data["published_at"] = _build_published_at(date_posted, posted_time_exact)

    return EnrichResult(job_id=job_id, url=url, status="success", data=data)


def _parse_job_page(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")

    desc_el = soup.find("div", class_="show-more-less-html__markup")
    if not desc_el:
        return None

    description_html = str(desc_el)
    description = md(description_html).strip()

    criteria = {}
    criteria_items = soup.find_all("li", class_="description__job-criteria-item")
    for item in criteria_items:
        header = item.find("h3")
        value = item.find("span", class_="description__job-criteria-text")
        if header and value:
            key = header.get_text(strip=True).lower().replace(" ", "_")
            criteria[key] = value.get_text(strip=True)

    salary = None
    salary_el = soup.find("div", class_="salary")
    if salary_el:
        salary = salary_el.get_text(strip=True)

    applicants = None
    applicants_el = soup.find("figcaption", class_="num-applicants__caption")
    if applicants_el:
        applicants = applicants_el.get_text(strip=True)

    apply_url = None
    apply_el = soup.find("code", id="applyUrl")
    if apply_el:
        match = re.search(r'(?<=\?url=)[^"]+', apply_el.get_text())
        if match:
            apply_url = match.group(0)

    # Top card
    company_name = None
    company_url = None
    company_link = soup.find("a", class_="topcard__org-name-link")
    if company_link:
        company_name = company_link.get_text(strip=True)
        if company_link.get("href"):
            company_url = company_link["href"].split("?")[0]

    location = None
    location_el = soup.find("span", class_="topcard__flavor--bullet")
    if location_el:
        location = location_el.get_text(strip=True)

    company_logo = None
    top_section = soup.find("section", class_="top-card-layout")
    if top_section:
        for img in top_section.find_all("img"):
            src = img.get("data-delayed-url", img.get("src", ""))
            if src and "company-logo" in src:
                company_logo = src
                break

    posted_time = None
    posted_el = soup.find("span", class_="posted-time-ago__text")
    if posted_el:
        posted_time = posted_el.get_text(strip=True)

    # Meta tags
    def _meta(name: str) -> str | None:
        tag = soup.find("meta", attrs={"name": name}) or soup.find("meta", attrs={"property": name})
        return tag["content"] if tag and tag.get("content") else None

    posted_time_exact = None
    meta_desc = _meta("description") or ""
    time_match = _POSTED_TIME_RE.search(meta_desc)
    if time_match:
        posted_time_exact = time_match.group(1)

    return {
        "description": description,
        "description_html": description_html,
        "company_name": company_name,
        "location": location,
        "seniority": criteria.get("seniority_level"),
        "employment_type": criteria.get("employment_type"),
        "job_function": criteria.get("job_function"),
        "industries": criteria.get("industries"),
        "applicants": applicants,
        "salary": salary,
        "apply_url": apply_url,
        "company_url": company_url,
        "company_logo": company_logo,
        "posted_time": posted_time,
        "posted_time_exact": posted_time_exact,
        "company_id": _meta("companyId"),
        "industry_ids": _meta("industryIds"),
        "title_id": _meta("titleId"),
        "canonical_url": _meta("lnkd:url"),
    }


def _build_published_at(date_posted: str, time_exact: str | None) -> str | None:
    if not date_posted or not time_exact:
        return None
    try:
        date_part = datetime.strptime(date_posted[:10], "%Y-%m-%d")
        time_part = datetime.strptime(time_exact.strip(), "%I:%M:%S %p")
        combined = date_part.replace(
            hour=time_part.hour, minute=time_part.minute, second=time_part.second
        )
        return combined.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, AttributeError):
        logger.debug("Could not build published_at from date_posted=%r, time_exact=%r", date_posted, time_exact)
        return None


def _build_headers(fingerprint: dict[str, str]) -> dict[str, str]:
    base = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    }
    if fingerprint:
        base.update(fingerprint)
    else:
        base["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        base["Accept-Language"] = "en-US,en;q=0.9"
    return base


def _compute_stats(
    results: list[EnrichResult],
    total_proxies: int,
    dead_proxies: dict[str, str],
    duration: float,
) -> EnrichStats:
    failed: dict[str, int] = {}
    success = 0
    for r in results:
        if r.status == "success":
            success += 1
        else:
            reason = r.error or r.status
            failed[reason] = failed.get(reason, 0) + 1

    return EnrichStats(
        total=len(results),
        success=success,
        failed=failed,
        proxies_total=total_proxies,
        proxies_alive=total_proxies - len(dead_proxies),
        proxy_deaths=dead_proxies,
        duration_seconds=round(duration, 1),
    )


def _result_to_dict(r: EnrichResult) -> dict:
    d = {"job_id": r.job_id, "url": r.url, "status": r.status}
    if r.data:
        d["data"] = r.data
    if r.error:
        d["error"] = r.error
    return d


def _empty_response(jobs: list[dict], error: str) -> dict:
    results = [
        _result_to_dict(EnrichResult(job_id=j.get("job_id", ""), url=j.get("url", ""), status="skipped", error=error))
        for j in jobs
    ]
    return {
        "results": results,
        "stats": EnrichStats(total=len(jobs), failed={error: len(jobs)}).to_dict(),
    }
