"""
Run Sumo Logic charge-request queries concurrently from a CSV (orderID, date).
Uses a thread pool to query multiple orders in parallel; prints results in CSV order.
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from base64 import b64encode
from http.cookiejar import CookieJar
import urllib.request

from sumoredrive.charge_request import (
    _run_one_order,
    _read_csv_order_dates,
    _next_day_from_time,
)


def _make_opener_and_headers(access_id, access_key):
    auth = b64encode(f"{access_id}:{access_key}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
    return opener, headers


def _run_one_order_task(order_id, from_time, to_time, timezone, api_url, sqs_queue_url, aws_region, access_id, access_key):
    """Run one order query (with optional next-day retry). Returns (order_id, printed, sent, results_list)."""
    opener, headers = _make_opener_and_headers(access_id, access_key)
    printed, sent, results = _run_one_order(
        order_id, from_time, to_time, timezone,
        opener, headers, api_url, sqs_queue_url, aws_region,
        return_results=True,
    )
    if printed == 0:
        next_range = _next_day_from_time(from_time, to_time)
        if next_range:
            next_from, next_to = next_range
            opener, headers = _make_opener_and_headers(access_id, access_key)
            printed, sent, results = _run_one_order(
                order_id, next_from, next_to, timezone,
                opener, headers, api_url, sqs_queue_url, aws_region,
                return_results=True,
            )
    return order_id, printed, sent, results or []


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Run Sumo Logic charge-request queries concurrently from a CSV (columns: orderID, date)."
    )
    parser.add_argument(
        "input",
        help="Path to CSV with columns: orderID, date (YYYY-MM-DD)",
    )
    parser.add_argument("--workers", type=int, default=4,
                        help="Max concurrent workers (default: 4)")
    parser.add_argument("--timezone", default="UTC", help="Time zone (default: UTC)")
    parser.add_argument("--sqs-queue-url", dest="sqs_queue_url", default=os.environ.get("SQS_QUEUE_URL"),
                        help="Send each charge request JSON to this SQS queue URL")
    args = parser.parse_args()

    access_id = os.environ.get("SUMO_ACCESS_ID")
    access_key = os.environ.get("SUMO_ACCESS_KEY")
    api_url = (os.environ.get("SUMO_API_URL") or "https://api.sumologic.com").rstrip("/")
    aws_region = os.environ.get("AWS_REGION")

    if not access_id or not access_key:
        print("Error: Set SUMO_ACCESS_ID and SUMO_ACCESS_KEY.", file=sys.stderr)
        sys.exit(1)

    if not os.path.isfile(args.input):
        print(f"Error: Not a file: {args.input!r}", file=sys.stderr)
        sys.exit(1)

    rows = _read_csv_order_dates(args.input)
    if not rows:
        print("Error: CSV has no valid rows (need at least two columns: orderID, date).", file=sys.stderr)
        sys.exit(1)

    timezone = args.timezone
    sqs_queue_url = args.sqs_queue_url
    print(f"Running {len(rows)} orders with {args.workers} workers.", file=sys.stderr, flush=True)
    start_time = time.perf_counter()

    # Submit all tasks; track index for ordered output
    n = len(rows)
    results_by_index = {}  # index -> (order_id, printed, sent, results)
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_index = {}
        for i, (order_id, date_str) in enumerate(rows):
            day_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            from_time = day_date.isoformat() + "T00:00:00"
            to_time = day_date.isoformat() + "T23:59:59"
            fut = executor.submit(
                _run_one_order_task,
                order_id, from_time, to_time, timezone,
                api_url, sqs_queue_url, aws_region,
                access_id, access_key,
            )
            future_to_index[fut] = i

        # As each order completes, print progress immediately
        for fut in as_completed(future_to_index):
            i = future_to_index[fut]
            order_id = rows[i][0]
            try:
                _order_id, printed, sent, results = fut.result()
                results_by_index[i] = (order_id, printed, sent, results)
                if printed == 0:
                    print(f"  [{i + 1}/{n}] {order_id}: not found", file=sys.stderr, flush=True)
                else:
                    print(f"  [{i + 1}/{n}] {order_id}: 1 result", file=sys.stderr, flush=True)
            except Exception as e:
                results_by_index[i] = (order_id, 0, 0, [])
                print(f"  [{i + 1}/{n}] {order_id}: Error: {e}", file=sys.stderr, flush=True)

    # Print JSON in CSV order
    total_printed = 0
    total_sent = 0
    not_found = []
    for i in range(n):
        order_id, printed, sent, results = results_by_index[i]
        total_printed += printed
        total_sent += sent
        if printed == 0:
            not_found.append(order_id)
        for obj in results:
            print(json.dumps(obj, indent=2), flush=True)

    elapsed = time.perf_counter() - start_time

    if not_found:
        print(file=sys.stderr, flush=True)
        print("Order IDs not found:", ", ".join(not_found), file=sys.stderr, flush=True)
    print(file=sys.stderr, flush=True)
    print(f"Messages published (to SQS): {total_sent}", file=sys.stderr, flush=True)
    print(f"Elapsed: {elapsed:.1f}s", file=sys.stderr, flush=True)

    if total_printed == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
