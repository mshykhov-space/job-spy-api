# job-spy-api

FastAPI wrapper around [python-jobspy](https://github.com/speedyapply/JobSpy). Scrapes LinkedIn, Indeed, Glassdoor, Google Jobs and more.

## Usage

```bash
docker run -p 8000:8000 mshykhov/job-spy-api
```

```
GET /jobs?site=linkedin&search_term=kotlin+developer&location=Europe&is_remote=true&hours_old=1&results_wanted=50&proxies=user:pass@host:port
POST /jobs/enrich
GET /health
```

`POST /jobs/enrich` runs one worker per proxy. Jobs from a dead or rate-limited proxy are
requeued for the remaining workers. If every proxy is exhausted, unprocessed jobs are returned
with `status: "skipped"` instead of hanging the request.

## Tests

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

## Release

```bash
git tag v0.1.0 && git push origin v0.1.0
```

Builds and pushes `mshykhov/job-spy-api:latest` to Docker Hub.
