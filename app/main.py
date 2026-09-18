import logging
import os
import threading
import tomllib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from jobspy import scrape_jobs
from pydantic import BaseModel, Field, SecretStr

from app.enrich import EnrichRequest as EnrichReq
from app.enrich import enrich_jobs


def _read_version() -> str:
    pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    if pyproject.exists():
        with open(pyproject, "rb") as f:
            return tomllib.load(f)["project"]["version"]
    return "0.0.0"


app = FastAPI(title="job-spy-api", version=_read_version())
logger = logging.getLogger(__name__)

DEFAULT_PROXIES = os.getenv("DEFAULT_PROXIES", "")
_SEARCH_LOCK = threading.Lock()


class SearchRequest(BaseModel):
    site: str
    search_term: str | None = None
    google_search_term: str | None = None
    location: str | None = None
    distance: int | None = Field(None, ge=1)
    job_type: str | None = None
    proxies: list[SecretStr] | None = None
    is_remote: bool = False
    results_wanted: int = Field(15, ge=1, le=1000)
    easy_apply: bool | None = None
    description_format: str = "markdown"
    offset: int | None = Field(None, ge=0)
    hours_old: int | None = Field(None, ge=1)
    verbose: int = Field(2, ge=0, le=2)
    linkedin_fetch_description: bool = False
    linkedin_company_ids: list[int] | None = None
    country_indeed: str = "usa"
    enforce_annual_salary: bool = False
    ca_cert: str | None = None


class _JobSpyFailureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.failed = False
        self.rate_limited = False

    def emit(self, record: logging.LogRecord) -> None:
        if not record.name.startswith("JobSpy:") or record.levelno < logging.WARNING:
            return
        self.failed = True
        message = record.getMessage().lower()
        self.rate_limited = (
            self.rate_limited or "429" in message or "rate limit" in message
        )


@contextmanager
def _capture_jobspy_failures() -> Iterator[_JobSpyFailureHandler]:
    handler = _JobSpyFailureHandler()
    with _SEARCH_LOCK:
        jobspy_loggers = [
            candidate
            for name, candidate in logging.root.manager.loggerDict.items()
            if name.startswith("JobSpy:") and isinstance(candidate, logging.Logger)
        ]
        states = [
            (candidate, list(candidate.handlers), candidate.propagate, candidate.level)
            for candidate in jobspy_loggers
        ]
        for candidate in jobspy_loggers:
            candidate.handlers = [handler]
            candidate.propagate = False
            candidate.setLevel(logging.WARNING)
        try:
            yield handler
        finally:
            for candidate, handlers, propagate, level in states:
                candidate.handlers = handlers
                candidate.propagate = propagate
                candidate.setLevel(level)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/jobs")
def search_jobs(
    site: str = Query(
        ...,
        description="Comma-separated: linkedin,indeed,google,glassdoor,zip_recruiter,bayt,bdjobs",
    ),
    search_term: str | None = Query(None),
    google_search_term: str | None = Query(
        None, description="Search term for Google Jobs only"
    ),
    location: str | None = Query(None),
    distance: int | None = Query(None, ge=1, description="In miles, default 50"),
    job_type: str | None = Query(
        None, description="fulltime, parttime, internship, contract"
    ),
    proxies: str | None = Query(
        None, description="Comma-separated: user:pass@host:port"
    ),
    is_remote: bool = Query(False),
    results_wanted: int = Query(15, ge=1, le=1000),
    easy_apply: bool | None = Query(
        None, description="Filter for jobs hosted on the job board site"
    ),
    description_format: str = Query("markdown", description="markdown or html"),
    offset: int | None = Query(
        None, ge=0, description="Start search from offset (e.g. 25)"
    ),
    hours_old: int | None = Query(
        None, ge=1, description="Filter by hours since posted"
    ),
    verbose: int = Query(2, ge=0, le=2, description="0=errors, 1=warnings, 2=all"),
    linkedin_fetch_description: bool = Query(
        False, description="Fetch full description for LinkedIn (O(n) extra requests)"
    ),
    linkedin_company_ids: str | None = Query(
        None, description="Comma-separated LinkedIn company IDs"
    ),
    country_indeed: str = Query("usa", description="Country for Indeed & Glassdoor"),
    enforce_annual_salary: bool = Query(
        False, description="Convert wages to annual salary"
    ),
    ca_cert: str | None = Query(None, description="Path to CA certificate for proxies"),
):
    request = SearchRequest(
        site=site,
        search_term=search_term,
        google_search_term=google_search_term,
        location=location,
        distance=distance,
        job_type=job_type,
        proxies=[SecretStr(value) for value in proxies.split(",")] if proxies else None,
        is_remote=is_remote,
        results_wanted=results_wanted,
        easy_apply=easy_apply,
        description_format=description_format,
        offset=offset,
        hours_old=hours_old,
        verbose=verbose,
        linkedin_fetch_description=linkedin_fetch_description,
        linkedin_company_ids=_parse_company_ids(linkedin_company_ids),
        country_indeed=country_indeed,
        enforce_annual_salary=enforce_annual_salary,
        ca_cert=ca_cert,
    )
    return _run_search(request)


@app.post("/jobs/search")
def strict_search(request: SearchRequest):
    return _run_search(request)


def _run_search(request: SearchRequest) -> list[dict[str, object]]:
    kwargs = _search_kwargs(request)
    with _capture_jobspy_failures() as failures:
        try:
            jobs = scrape_jobs(**kwargs)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "invalid_search",
                    "message": "Job search parameters are invalid",
                },
            )
        except Exception:  # noqa: BLE001 - sanitize arbitrary third-party failures
            logger.error("Job search failed")
            raise HTTPException(
                status_code=502,
                detail={
                    "code": "upstream_failure",
                    "message": "Job search upstream failed",
                },
            )

    if failures.failed:
        if failures.rate_limited:
            raise HTTPException(
                status_code=429,
                detail={
                    "code": "upstream_rate_limited",
                    "message": "Job search upstream was rate limited",
                },
            )
        raise HTTPException(
            status_code=502,
            detail={
                "code": "upstream_failure",
                "message": "Job search upstream failed",
            },
        )

    jobs = jobs.where(jobs.notna(), None)
    return jobs.to_dict(orient="records")


def _search_kwargs(request: SearchRequest) -> dict[str, object]:
    proxies = (
        [value.get_secret_value() for value in request.proxies]
        if request.proxies is not None
        else DEFAULT_PROXIES.split(",")
        if DEFAULT_PROXIES
        else None
    )
    kwargs: dict[str, object] = {
        "site_name": request.site.split(","),
        "search_term": request.search_term,
        "google_search_term": request.google_search_term,
        "location": request.location,
        "distance": request.distance,
        "job_type": request.job_type,
        "proxies": _normalize_proxies(proxies),
        "is_remote": request.is_remote,
        "results_wanted": request.results_wanted,
        "description_format": request.description_format,
        "hours_old": request.hours_old,
        "verbose": request.verbose,
        "linkedin_fetch_description": request.linkedin_fetch_description,
        "country_indeed": request.country_indeed,
        "enforce_annual_salary": request.enforce_annual_salary,
    }
    optional = {
        "easy_apply": request.easy_apply,
        "offset": request.offset,
        "linkedin_company_ids": request.linkedin_company_ids,
        "ca_cert": request.ca_cert,
    }
    kwargs.update({key: value for key, value in optional.items() if value is not None})
    return kwargs


class EnrichJobItem(BaseModel):
    url: str
    job_id: str = ""
    date_posted: str = ""


class EnrichProxyItem(BaseModel):
    url: str
    fingerprint: dict[str, str] = {}


class EnrichRequestBody(BaseModel):
    jobs: list[EnrichJobItem]
    proxies: list[EnrichProxyItem]
    delay_min: int = 7
    delay_max: int = 15


@app.post("/jobs/enrich")
async def enrich(body: EnrichRequestBody):
    request = EnrichReq(
        jobs=[j.model_dump() for j in body.jobs],
        proxies=[p.model_dump() for p in body.proxies],
        delay_min=body.delay_min,
        delay_max=body.delay_max,
    )
    return await enrich_jobs(request)


def _normalize_proxies(proxies: list[str] | None) -> list[str] | None:
    if not proxies:
        return None
    return [proxy.removeprefix("http://").removeprefix("https://") for proxy in proxies]


def _parse_company_ids(ids: str | None) -> list[int] | None:
    if not ids:
        return None
    return [int(x.strip()) for x in ids.split(",") if x.strip().isdigit()]
