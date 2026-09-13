# Job Spy API

FastAPI wrapper around [python-jobspy](https://github.com/speedyapply/JobSpy) for searching job boards and enriching individual LinkedIn job pages through caller-supplied proxies.

`GET /jobs` forwards supported search parameters to python-jobspy. `POST /jobs/enrich` processes a batch with one worker per proxy, requeues work after a proxy failure, and returns skipped jobs when every proxy is exhausted. `GET /health` returns the service status.

## Run locally

Requires Python 3.12 or Docker.

```sh
docker build -t job-spy-api .
docker run --rm -p 8000:8000 job-spy-api
```

The service accepts proxy credentials through `DEFAULT_PROXIES` or the request. Do not expose them in URLs, logs, or committed environment files.

## Test

```sh
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Tests mock network access.

## License

[MIT](LICENSE)
