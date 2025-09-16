## What I built, why I built it this way, and how I’d run it on Azure

I built a production-ready ETL: read movie IDs from CSV, fetch facts from TMDB, transform them into a stable schema, and ship a stakeholder-ready Excel. I expect messy inputs, so I validate hard up front, retry transient API issues, and make failures auditable via a dead-letter queue (DLQ). Final output is sorted by ID so diffs and reviews stay stable across runs.

> The DLQ wasn’t in the original assignment given, but I thought it would be considered good practice. Easy to just delete this DLQ implementation if not needed as well. 

## Stack & structure

* **Python 3.12**
* **requests** + `ThreadPoolExecutor` for resilient, parallel I/O
* **python-dotenv** for config (`TMDB_API_KEY`)
* **openpyxl** for controlled Excel formatting & atomic saves
* **pytest** with mocks (no live API needed)

```
├── src/
│   ├── __init__.py
│   ├── tmdb_client.py                 # TMDBClient: timeouts, retries, backoff+jitter
│   ├── report_io.py                   # read_movie_ids_with_skips, write_excel, write_failed_ids
│   ├── schemas.py                     # MovieRow dataclass (id, title, vote_average, genres, is_action)
│   └── movie_report_pipeline.py       # Orchestration: concurrency, progress, summary logs
├── tests/                          
│   ├── conftest.py                    # Test configuration: adds src/ to Python path
│   ├── test_movie_report_pipeline.py  # Tests make_movie_row transformation logic
│   ├── test_report_io.py              # Tests CSV reading, Excel writing, DLQ handling
│   └── test_tmdb_client.py            # Tests API client retry/timeout behavior
├── requirements.txt
├── requirements-dev.txt
├── movies.csv
└── README.md
```

---

## How I approached it

**API contract first.** In Postman I called the movie-details endpoint for a known ID (`634649`) to lock down fields I actually need (title, vote\_average, genres). I used a Postman environment variable for the API key so secrets never hit the URL or repo.

```http
GET https://api.themoviedb.org/3/movie/634649?language=en-US
# Auth: api_key stored as {{tmdb_api_key}} in Postman env
```

**Configuration & client.** I load `TMDB_API_KEY` from `.env` and fail fast if it’s missing. `TMDBClient` wraps a single `requests.Session`, uses a 10-second timeout, and retries up to 3 times with exponential backoff + jitter. I retry 429/5xx/network errors; I treat 404 as terminal and log it.

---

## Clean input → clean output (ETL mapping)

### Extract (E): validate & de-dupe IDs

* `read_movie_ids_with_skips(csv_path)` reads `movies.csv` (requires header `ID`, handles UTF-8 BOM).
* I only accept **positive integers** and **de-dupe** with a set.
* Every reject is tracked as `(raw_value, row_number, reason)` where reason ∈ `{blank, non-numeric, non-positive, duplicate}`.
* Example: `399S79` on row 117 logs as `non-numeric`.

### Transform (T): TMDB JSON → `MovieRow`

* `make_movie_row(movie_id, api_data)` maps to `MovieRow(id, title, vote_average, genres, is_action)`.
* Defensive parsing: safe numeric casts, tolerate missing fields.
* Genres are normalized to names and **alphabetically sorted** for deterministic output.
* Simple business rule: `is_action = "Action" in genres`.

### Load (L): Excel + DLQ

* Rows are **sorted by ID** and written via `write_excel(...)` with:

  * Header: `ID, Title, Vote Average, Genres`
  * Frozen header row (`A2`)
  * Full-row **bold + red** highlight when `is_action` is `True`
  * **Atomic save**: write to temp, then `os.replace` to `movie_data.xlsx`
* Failures & skips go to `write_failed_ids(...)` → `dead_letter_queue.csv` with columns `ID, Reason, RowNumber`.

  * API failures: `[id, "fetch-failed", ""]`
  * Input skips: `[raw, reason, row]`

---

## Orchestration & performance

* `movie_report_pipeline.py` is the entrypoint.
* I fan out fetches with `ThreadPoolExecutor(max_workers=10)` and track progress with `tqdm`.
* `TMDBClient.fetch_movie_details(movie_id)` handles retries/backoff. 404 returns `None` immediately.
* I log a summary with processed/failed/skipped counts, elapsed seconds, and approximate requests/sec.

Why threads over `asyncio`? For straight HTTP I/O at this size, threads are simpler, easier to test, and “fast enough.” If quotas tighten, I’d add a token-bucket limiter on top.

---

## Example

ID `634649` → “Spider-Man: No Way Home,” vote average 7.94, genres “Action, Adventure, Science Fiction.” The row is emphasized (bold+red) because “Action” is present. Flow: **raw API → stable model → simple business rule → formatted report**.

---

## Rationale & Impact

* **Strict input validation** → less wasted API traffic and fewer surprises (cheaper, faster).
* **Timeouts + retry/backoff; 404 terminal** → unattended success on flaky networks without burning time on non-existent IDs (reliable, cost-aware).
* **Capped concurrency** → good throughput without rate-limit storms (faster, polite).
* **Deterministic sorting** → stable diffs and simpler reviews (lower ops friction).
* **Atomic Excel writes** → never publish a partial or corrupted file (safer).
* **DLQ** → failures are visible and re-runnable (operable).
* **Separation of concerns** → client/I-O/schema/orchestration stay testable and easy to change.

Net effect: **faster runs, fewer manual touches, cheaper retries, and clear accountability** when something goes wrong.


## Tradeoffs I considered

* **Threads vs asyncio:** I stayed with threads for simplicity; I could swap to `asyncio+httpx` if latency/throughput demands it.
* **Pandas vs openpyxl:** I chose openpyxl for precise formatting + atomic writes; if transforms grow, I'd stage a DataFrame then style via openpyxl.
* **Rate limiting:** I currently rely on capped workers + backoff; I would add a token bucket if TMDB quotas tighten.
* **Metadata:** I included core columns in Excel; I could easily add `imdb_id`, `release_date`, or `runtime` if needed.

---


---

## Testing

I use `pytest` with mocks—no real network or API key required.

* CSV edge cases (blanks, non-numeric, duplicates)
* Retry/backoff paths (429, 5xx, network exception)
* Transform correctness & deterministic genre order
* Excel formatting (action highlight) and atomic write behavior
* DLQ content (API failures + input skips)

---

## How I’d run this on Azure

I’d keep the code thin, observable, and idempotent.

* **Landing**: ADLS Gen2 (CSV input; optionally persist raw TMDB JSON for lineage).
  Secrets in **Key Vault** via managed identity; RBAC on Storage/SQL.
* **Orchestration**: **Azure Data Factory** for schedule, parameters, lineage, run history, one-click re-runs.
* **Compute**: **Azure Function (Python)** hosts the fetch/transform, enforces token-bucket rate limiting, and uses the same retry/backoff and capped concurrency as local.
* **Load**:

  * Excel to Blob for download, **and/or**
  * **Azure SQL** with `MERGE` on `movie_id` (optionally include a payload hash) so re-runs are idempotent.
* **Re-runs & DLQ**: send `{id, reason, attempt, last_error}` to **Service Bus**; a retry Function drains the queue with a max-attempt policy. Exhausted items persist to a DLQ table for triage.
* **Schema drift**: version raw JSON; keep mapper tolerant; promote new columns intentionally.
* **Monitoring**: App Insights + ADF run history; alerts on spikes or Service Level Agreement (SLA) risk.

**Why ADF + Functions instead of using Logic Apps?:** ADF gives data-pipeline ergonomics (lineage, parameters, re-runs). Functions give precise control of HTTP and throttling. Logic Apps are great for notifications (e.g., emailing a SAS link) but not the data plane here.

---

## Runbook

### 1) Tests (dev)

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pytest -q
```

### 2) Run the pipeline (local)

```bash
pip install -r requirements.txt
echo "TMDB_API_KEY=YOUR_TMDB_KEY" > .env            # or: export TMDB_API_KEY=YOUR_TMDB_KEY
python -m src.movie_report_pipeline                 # expects movies.csv with header: ID
```

**Outputs**

* `movie_data.xlsx` — formatted, deterministic report
* `dead_letter_queue.csv` — API failures + input skips with reasons
---
