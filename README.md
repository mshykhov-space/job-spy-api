# Job Spy API

FastAPI wrapper around [python-jobspy](https://github.com/speedyapply/JobSpy) for searching job boards and enriching individual LinkedIn job pages through caller-supplied proxies.

`POST /jobs/search` forwards supported search parameters and proxies to python-jobspy without placing credentials in the URL. It returns an array on a complete search, including a genuine short terminal page, and returns a typed `429` or `502` response when python-jobspy reports a rate limit or another upstream failure. Searches are serialized so each request observes only its own upstream log signals.

`GET /jobs` remains available with the same successful array response for existing clients. `POST /jobs/enrich` processes a batch with one worker per proxy and requeues work after a proxy failure. A proxy that receives HTTP 429 enters a process-local cooldown using `Retry-After` (delta-seconds or HTTP date), defaulting to 60 seconds when the header is missing or invalid; later fetches and batches skip it until the requested cooldown expires. Jobs remain skipped and unresolved when no eligible proxy can process them. `GET /health` returns the service status.

```sh
curl -X POST http://localhost:8000/jobs/search \
  -H 'Content-Type: application/json' \
  -d '{
    "site": "linkedin",
    "search_term": "Kotlin",
    "location": "Europe",
    "is_remote": true,
    "results_wanted": 100,
    "offset": 0,
    "hours_old": 24,
    "proxies": ["http://user:password@proxy.example:8000"]
  }'
```

## Run locally

Requires Python 3.12 or Docker.

```sh
docker build -t job-spy-api .
docker run --rm -p 8000:8000 job-spy-api

# Published image
docker run --rm -p 8000:8000 ghcr.io/mshykhov-space/job-spy-api:latest
```

The service accepts proxy credentials through `DEFAULT_PROXIES` or the POST request body. The legacy GET query is retained for compatibility. Do not expose credentials in URLs, logs, or committed environment files.

## Test

```sh
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Tests mock network access.

## License

[MIT](LICENSE)
