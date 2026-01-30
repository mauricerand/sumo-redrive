# sumoRedrive

Query Sumo Logic for charge requests by order ID, pretty-print JSON, and optionally send to SQS. Input: single order ID (with `--day`) or a CSV with columns `orderID` and `date`.

## Install

```bash
cd sumoRedrive
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
```

For SQS support (when using `--sqs-queue-url`):

```bash
pip install -e ".[sqs]"
```

## Usage

**Single order ID** (use `--day` or set `SUMO_DAY`):

```bash
sumo-redrive <ORDER_ID> --day YYYY-MM-DD
```

**CSV file** (columns: `orderID`, `date` in YYYY-MM-DD; optional header `orderID,date`):

```bash
sumo-redrive orders.csv
sumo-redrive orders.csv --sqs-queue-url https://sqs....
```

**Run as module:**

```bash
python -m sumoredrive <ORDER_ID> --day YYYY-MM-DD
python -m sumoredrive orders.csv
```

**Concurrent (CSV only)** – run multiple orders in parallel with a thread pool:

```bash
sumo-redrive-concurrent orders.csv
sumo-redrive-concurrent orders.csv --workers 10 --sqs-queue-url https://sqs....
python -m sumoredrive.run_concurrent orders.csv --workers 5
```

### Examples

```bash
export SUMO_ACCESS_ID=your_id SUMO_ACCESS_KEY=your_key
sumo-redrive ee4938b392426b97dcae85 --day 2026-01-20
sumo-redrive orders.csv --sqs-queue-url https://sqs.us-east-1.amazonaws.com/123456789012/my-queue
sumo-redrive 12345678 --from -1d   # single order, relative time
```

## Environment

- `SUMO_ACCESS_ID` – Sumo Logic access ID (required)
- `SUMO_ACCESS_KEY` – Sumo Logic access key (required)
- `SUMO_API_URL` – API base, e.g. `https://api.sumologic.com` (optional)
- `SUMO_FROM` / `SUMO_TO` – search time range (optional; default `-7d` / `now`)
- `SUMO_DAY` – day to search, YYYY-MM-DD (optional)
- `SQS_QUEUE_URL` – SQS queue URL when using `--sqs-queue-url` (optional)
- `AWS_REGION` – AWS region for SQS (optional)

Create [Sumo Logic access keys](https://help.sumologic.com/docs/manage-security/access-keys/) with **Data Management** (Download Search Results, View Collectors) and **Security** (Manage Access Keys).

## Behavior

- Runs the charge-request query in Sumo, parses the first result, and pretty-prints JSON to stdout.
- If no result for the given day, retries the next calendar day once.
- With `--sqs-queue-url`, sends each charge request JSON to the queue (requires `pip install -e ".[sqs]"`).
- At the end prints: order IDs not found (if any), messages published to SQS, and elapsed time.

**Concurrent script** (`sumo-redrive-concurrent`): accepts only a CSV path; runs one Sumo query per row in a thread pool (default 10 workers). Results are printed in CSV order. Use for faster batch runs.
