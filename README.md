# sumo-redrive

Query Sumo Logic for charge requests by order ID, pretty-print JSON, and optionally send to SQS.

**Input:** Single order ID (with `--day` or relative time) or a CSV with columns `orderID` and `date`.

## Install

```bash
cd sumo-redrive
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
```

For SQS support (when using `--sqs-queue-url`):

```bash
pip install -e ".[sqs]"
```

## Usage

### Single order ID

**By day** (recommended):

```bash
sumo-redrive <ORDER_ID> --day YYYY-MM-DD
```

**By relative time** (`--from` / `--to`; converted to ISO 8601 for the Sumo API):

```bash
sumo-redrive <ORDER_ID> --from -7d              # last 7 days to now
sumo-redrive <ORDER_ID> --from -1d --to now     # last 1 day to now
sumo-redrive <ORDER_ID> --from -12h             # last 12 hours (h, m, s supported)
```

Relative `--from`: `-Nd`, `-Nh`, `-Nm`, `-Ns` (days, hours, minutes, seconds). `--to` defaults to `now`.

### CSV file

Columns: `orderID`, `date` (YYYY-MM-DD). Optional header row: `orderID,date` or `Order ID,Date`.

```bash
sumo-redrive orders.csv
sumo-redrive orders.csv --sqs-queue-url https://sqs....
sumo-redrive orders.csv --timezone America/New_York
```

### Concurrent (CSV only)

Run multiple orders in parallel with a thread pool. Progress lines appear on stderr as each order completes; JSON is printed to stdout in CSV order.

```bash
sumo-redrive-concurrent orders.csv
sumo-redrive-concurrent orders.csv --workers 4 --sqs-queue-url https://sqs....
python -m sumoredrive.run_concurrent orders.csv --workers 5
```

### Run as module

```bash
python -m sumoredrive <ORDER_ID> --day YYYY-MM-DD
python -m sumoredrive orders.csv
```

## Options

| Option | Description |
|--------|-------------|
| `--day YYYY-MM-DD` | Day to search (single order only). |
| `--from`, `--to` | Time range: ISO 8601 or relative (`-7d`, `-1d`, `-12h`, `now`). Default: `-7d` / `now`. |
| `--timezone` | Time zone for from/to (default: UTC). |
| `--sqs-queue-url URL` | Send each charge request JSON to this SQS queue. |
| `--debug` | Print query and time range to stderr, then exit. |
| `--workers N` | (Concurrent only) Max parallel workers (default: 4). |

## Environment

| Variable | Description |
|----------|-------------|
| `SUMO_ACCESS_ID` | Sumo Logic access ID (required). |
| `SUMO_ACCESS_KEY` | Sumo Logic access key (required). |
| `SUMO_API_URL` | API base, e.g. `https://api.sumologic.com` (optional). |
| `SUMO_FROM` / `SUMO_TO` | Default time range, e.g. `-7d` / `now` (optional). |
| `SUMO_DAY` | Default day, YYYY-MM-DD (optional). |
| `SQS_QUEUE_URL` | SQS queue URL when using `--sqs-queue-url` (optional). |
| `AWS_REGION` | AWS region for SQS (optional). |

Create [Sumo Logic access keys](https://help.sumologic.com/docs/manage-security/access-keys/) with **Data Management** (Download Search Results, View Collectors) and **Security** (Manage Access Keys).

## Behavior

- Runs the Sumo charge-request query, parses the **first** result, and pretty-prints JSON to stdout.
- If no result for the given day, retries the **next calendar day** once.
- **Relative time** (`-7d`, `-1d`, `now`) is converted to ISO 8601 UTC before calling the Sumo API.
- With `--sqs-queue-url`, sends each charge request JSON to the queue (requires `pip install -e ".[sqs]"`).
- At the end (stderr): **Order IDs not found** (if any), **Messages published (to SQS)**, and **Elapsed** time.

**Concurrent script** (`sumo-redrive-concurrent`): CSV only; one Sumo query per row in a thread pool. Progress lines (e.g. `[3/10] order_id: 1 result`) on stderr as each order completes; JSON on stdout in CSV order.
