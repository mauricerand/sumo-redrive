"""
Run Sumo Logic charge-request query by order ID and pretty-print the extracted JSON.
Optionally send each JSON payload to an SQS queue.

Input: single order ID (with --day) or a CSV file with columns: orderID, date.
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from base64 import b64encode
import re
from datetime import datetime, timedelta, timezone
from http.cookiejar import CookieJar
from urllib.parse import urlencode

# Query template: ORDER_ID is replaced with the actual order ID (single line for API).
# In Sumo replace(), one backslash must be escaped as \\\\ (four backslashes in query).
QUERY_TEMPLATE = (
    '_dataTier=infrequent _index=nytimes_spg_shared _sourceCategory=nytimes-spg-pug-app-prd '
    '"PUGRB: Received charge request" "%(order_id)s" '
    '| parse regex "request (?<json>.*), approximate" '
    '| replace(json, "\\\\\\\\", "") as json'
)

POLL_INTERVAL_SEC = 2
MESSAGE_PAGE_SIZE = 1000


def _send_to_sqs(queue_url, body, region=None):
    """Send a message to SQS. Requires boto3."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        print("Error: boto3 is required for --sqs-queue-url. Install with: pip install sumoredrive[sqs]", file=sys.stderr)
        sys.exit(1)
    client_kw = {}
    if region:
        client_kw["region_name"] = region
    sqs = boto3.client("sqs", **client_kw)
    try:
        sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "AWS.SimpleQueueService.NonExistentQueue":
            raise SystemExit(
                "SQS queue not found. Check:\n"
                "  - Queue URL is the full URL (e.g. https://sqs.us-east-1.amazonaws.com/123456789012/queue-name)\n"
                "  - Region matches (--sqs-queue-url host or AWS_REGION)\n"
                "  - Queue exists in this AWS account (AWS Console → SQS → open queue → copy URL)"
            ) from e
        raise


def _request(opener, url, headers, data=None, method=None):
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with opener.open(req) as resp:
        return resp.status, resp.read().decode()


def _read_csv_order_dates(path):
    """Read CSV with columns orderID, date. Returns list of (order_id, date_str). Skips header if present."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if len(row) < 2:
                continue
            order_id = row[0].strip()
            date_str = row[1].strip()
            if not order_id or not date_str:
                continue
            # Skip header row
            if i == 0 and (
                order_id.lower() in ("orderid", "order_id", "order id") or date_str.lower() == "date"
            ):
                continue
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                print(f"Warning: skip row (invalid date {date_str!r}): {row}", file=sys.stderr)
                continue
            rows.append((order_id, date_str))
    return rows


def _run_one_order(order_id, from_time, to_time, timezone, opener, headers, api_url, sqs_queue_url, aws_region, return_results=False):
    """Run Sumo query for one order_id and time range.
    Returns (printed_count, sent_to_sqs_count) or, when return_results=True, (printed_count, sent_to_sqs_count, list_of_parsed_dicts).
    """
    query = QUERY_TEMPLATE % {"order_id": order_id}
    search_job = {
        "query": query,
        "from": from_time,
        "to": to_time,
        "timeZone": timezone,
    }
    body = json.dumps(search_job).encode()
    try:
        status, text = _request(
            opener, f"{api_url}/api/v1/search/jobs", headers, data=body, method="POST"
        )
    except urllib.error.HTTPError as e:
        print(f"Error creating search job: {e.code}", e.read().decode(), file=sys.stderr)
        return (0, 0, []) if return_results else (0, 0)
    if status != 202:
        print(f"Error creating search job: {status}", text, file=sys.stderr)
        return (0, 0, []) if return_results else (0, 0)
    job_id = json.loads(text)["id"]
    if not return_results:
        print(f"[{order_id}] Search job created: {job_id}", file=sys.stderr)

    while True:
        time.sleep(POLL_INTERVAL_SEC)
        try:
            status, text = _request(
                opener, f"{api_url}/api/v1/search/jobs/{job_id}", headers
            )
        except urllib.error.HTTPError as e:
            print(f"Error getting job status: {e.code}", e.read().decode(), file=sys.stderr)
            return (0, 0, []) if return_results else (0, 0)
        if status != 200:
            print(f"Error getting job status: {status}", text, file=sys.stderr)
            return (0, 0, []) if return_results else (0, 0)
        resp = json.loads(text)
        state = resp.get("state", "")
        message_count = resp.get("messageCount", 0)
        if state == "DONE GATHERING RESULTS":
            break
        if message_count >= 1:
            # We only use the first result; no need to wait for job to finish
            break
        if state == "CANCELLED":
            print("Search job was cancelled.", file=sys.stderr)
            return (0, 0, []) if return_results else (0, 0)
        if not return_results:
            print(f"  State: {state}, messages: {message_count}", file=sys.stderr)

    printed = 0
    sent_to_sqs = 0
    results_list = [] if return_results else None
    offset = 0
    while offset < message_count:
        url = f"{api_url}/api/v1/search/jobs/{job_id}/messages?{urlencode({'offset': offset, 'limit': MESSAGE_PAGE_SIZE})}"
        try:
            status, text = _request(opener, url, headers)
        except urllib.error.HTTPError as e:
            print(f"Error fetching messages: {e.code}", e.read().decode(), file=sys.stderr)
            break
        if status != 200:
            print(f"Error fetching messages: {status}", text, file=sys.stderr)
            break
        data = json.loads(text)
        messages = data.get("messages", [])
        if not messages:
            break
        for msg in messages:
            m = msg.get("map", {})
            raw_json = m.get("json")
            if not raw_json:
                continue
            obj = None
            try:
                obj = json.loads(raw_json)
                if isinstance(obj, str):
                    obj = json.loads(obj)
            except json.JSONDecodeError:
                try:
                    unescaped = raw_json.replace("\\\"", "\"").replace("\\\\", "\\")
                    obj = json.loads(unescaped)
                except json.JSONDecodeError:
                    print(raw_json, file=sys.stderr)
                    continue
            if obj is not None:
                if return_results:
                    results_list.append(obj)
                else:
                    out = json.dumps(obj, indent=2)
                    if printed > 0:
                        print()
                    print(out)
                printed += 1
                if sqs_queue_url:
                    try:
                        _send_to_sqs(sqs_queue_url, json.dumps(obj), region=aws_region)
                        sent_to_sqs += 1
                        if not return_results:
                            print(f"  -> sent to SQS ({sent_to_sqs})", file=sys.stderr)
                    except Exception as e:
                        print(f"  -> SQS send failed: {e}", file=sys.stderr)
                # Accept only the first result
                break
        if printed > 0:
            break
        offset += len(messages)

    try:
        _request(opener, f"{api_url}/api/v1/search/jobs/{job_id}", headers, method="DELETE")
    except (urllib.error.HTTPError, OSError):
        pass
    if return_results:
        return printed, sent_to_sqs, results_list
    return printed, sent_to_sqs


def _resolve_relative_time(from_time, to_time):
    """Convert relative time (-7d, -1d, -12h, now) to ISO 8601 UTC. Sumo API accepts only ISO 8601 or epoch."""
    now_utc = datetime.now(timezone.utc)
    to_str = (to_time or "now").strip().lower()
    to_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%S") if to_str == "now" else to_time
    from_iso = from_time
    resolved_tz = None
    # Parse relative from: -7d, -1d, -12h, -30m, -60s
    m = re.match(r"^-(\d+)([dhms])$", (from_time or "").strip().lower())
    if m:
        amount = int(m.group(1))
        unit = m.group(2)
        if unit == "d":
            delta = timedelta(days=amount)
        elif unit == "h":
            delta = timedelta(hours=amount)
        elif unit == "m":
            delta = timedelta(minutes=amount)
        else:
            delta = timedelta(seconds=amount)
        from_iso = (now_utc - delta).strftime("%Y-%m-%dT%H:%M:%S")
        resolved_tz = "UTC"
    return from_iso, to_iso, resolved_tz


def _next_day_from_time(from_time, to_time):
    """If from_time/to_time are same-day (YYYY-MM-DDTHH:MM:SS), return (next_from, next_to). Else None."""
    if len(from_time) < 10 or from_time[:4].isdigit() is False:
        return None
    try:
        day_date = datetime.strptime(from_time[:10], "%Y-%m-%d").date()
        next_day = day_date + timedelta(days=1)
        next_from = next_day.isoformat() + "T00:00:00"
        next_to = next_day.isoformat() + "T23:59:59"
        return next_from, next_to
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Query Sumo Logic for charge request by order ID and pretty-print JSON. "
        "Input: order ID (with --day) or CSV path with columns orderID, date."
    )
    parser.add_argument(
        "input",
        help="Order ID (string) or path to CSV with columns: orderID, date (YYYY-MM-DD)",
    )
    parser.add_argument("--day", dest="day", default=os.environ.get("SUMO_DAY"),
                        help="Day to search, YYYY-MM-DD (for single order ID only)")
    parser.add_argument("--from", dest="from_time", default=os.environ.get("SUMO_FROM", "-7d"),
                        help="Search start (ISO 8601 or relative e.g. -7d); ignored if --day set")
    parser.add_argument("--to", dest="to_time", default=os.environ.get("SUMO_TO"),
                        help="Search end (ISO 8601); default now; ignored if --day set")
    parser.add_argument("--timezone", default="UTC", help="Time zone for from/to (default: UTC)")
    parser.add_argument("--debug", action="store_true", help="Print the query and time range sent to Sumo (then exit)")
    parser.add_argument("--sqs-queue-url", dest="sqs_queue_url", default=os.environ.get("SQS_QUEUE_URL"),
                        help="Send each charge request JSON to this SQS queue URL (or set SQS_QUEUE_URL)")
    args = parser.parse_args()

    access_id = os.environ.get("SUMO_ACCESS_ID")
    access_key = os.environ.get("SUMO_ACCESS_KEY")
    api_url = (os.environ.get("SUMO_API_URL") or "https://api.sumologic.com").rstrip("/")

    if not access_id or not access_key:
        print("Error: Set SUMO_ACCESS_ID and SUMO_ACCESS_KEY.", file=sys.stderr)
        sys.exit(1)

    timezone = args.timezone
    sqs_queue_url = args.sqs_queue_url
    aws_region = os.environ.get("AWS_REGION")

    if os.path.isfile(args.input):
        # CSV mode: each row is (order_id, date)
        rows = _read_csv_order_dates(args.input)
        if not rows:
            print("Error: CSV has no valid rows (need at least two columns: orderID, date).", file=sys.stderr)
            sys.exit(1)
        if args.debug:
            order_id, date_str = rows[0]
            day_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            from_time = day_date.isoformat() + "T00:00:00"
            to_time = day_date.isoformat() + "T23:59:59"
            query = QUERY_TEMPLATE % {"order_id": order_id}
            search_job = {"query": query, "from": from_time, "to": to_time, "timeZone": timezone}
            print("Query (first row):", repr(query), file=sys.stderr)
            print("Search job:", json.dumps(search_job, indent=2), file=sys.stderr)
            print(f"Total rows: {len(rows)}", file=sys.stderr)
            sys.exit(0)
        auth = b64encode(f"{access_id}:{access_key}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
        total_printed = 0
        total_sent = 0
        not_found = []
        start_time = time.perf_counter()
        for i, (order_id, date_str) in enumerate(rows):
            day_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            from_time = day_date.isoformat() + "T00:00:00"
            to_time = day_date.isoformat() + "T23:59:59"
            if i > 0:
                print(file=sys.stderr)
            printed, sent = _run_one_order(
                order_id, from_time, to_time, timezone,
                opener, headers, api_url, sqs_queue_url, aws_region,
            )
            if printed == 0:
                next_range = _next_day_from_time(from_time, to_time)
                if next_range:
                    next_from, next_to = next_range
                    print(f"[{order_id}] No result for {date_str}, trying next day.", file=sys.stderr)
                    printed, sent = _run_one_order(
                        order_id, next_from, next_to, timezone,
                        opener, headers, api_url, sqs_queue_url, aws_region,
                    )
            total_printed += printed
            total_sent += sent
            if printed == 0:
                not_found.append(order_id)
        elapsed = time.perf_counter() - start_time
        if not_found:
            print(file=sys.stderr)
            print("Order IDs not found:", ", ".join(not_found), file=sys.stderr)
        print(file=sys.stderr)
        print(f"Messages published (to SQS): {total_sent}", file=sys.stderr)
        print(f"Elapsed: {elapsed:.1f}s", file=sys.stderr)
        if total_printed == 0:
            sys.exit(1)
        sys.exit(0)

    # Single order ID mode
    order_id = args.input
    if args.day:
        try:
            day_date = datetime.strptime(args.day, "%Y-%m-%d").date()
        except ValueError:
            print(f"Error: --day must be YYYY-MM-DD, got: {args.day!r}", file=sys.stderr)
            sys.exit(1)
        from_time = day_date.isoformat() + "T00:00:00"
        to_time = day_date.isoformat() + "T23:59:59"
    else:
        from_time = args.from_time
        to_time = args.to_time or "now"
        from_time, to_time, resolved_tz = _resolve_relative_time(from_time, to_time)
        if resolved_tz is not None:
            timezone = resolved_tz

    query = QUERY_TEMPLATE % {"order_id": order_id}
    search_job = {
        "query": query,
        "from": from_time,
        "to": to_time,
        "timeZone": timezone,
    }

    if args.debug:
        print("Query:", repr(query), file=sys.stderr)
        print("Search job:", json.dumps(search_job, indent=2), file=sys.stderr)
        sys.exit(0)

    auth = b64encode(f"{access_id}:{access_key}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
    start_time = time.perf_counter()

    printed, sent = _run_one_order(
        order_id, from_time, to_time, timezone,
        opener, headers, api_url, sqs_queue_url, aws_region,
    )
    next_range = _next_day_from_time(from_time, to_time)
    if printed == 0 and next_range:
        next_from, next_to = next_range
        print(f"No result for --day, trying next day.", file=sys.stderr)
        printed, sent = _run_one_order(
            order_id, next_from, next_to, timezone,
            opener, headers, api_url, sqs_queue_url, aws_region,
        )
    elapsed = time.perf_counter() - start_time
    if printed == 0:
        print(file=sys.stderr)
        print("Order IDs not found:", order_id, file=sys.stderr)
        sys.exit(1)
    print(file=sys.stderr)
    print(f"Messages published (to SQS): {sent}", file=sys.stderr)
    print(f"Elapsed: {elapsed:.1f}s", file=sys.stderr)
