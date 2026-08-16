"""
Salesforce Data Cloud — Monthly Forecast Full Load
Ingests 60 months of data (Jul 2020 - Jun 2025) into a Data Cloud
Data Stream using the Ingestion API (no 5MB storage cap).

Prerequisites — do these once in Data Cloud UI before running:
  1. Setup → Data Cloud → Data Streams → New → "Ingestion API"
  2. Name it: HPE_Forecast_Monthly
  3. Add all fields matching the schema below (STRING / DATE / NUMBER)
  4. The DLO (Data Lake Object) auto-created = HPE_Forecast_Monthly__dll
  5. Setup → Data Cloud → Connected Apps → create an OAuth connected app
     with scope: cdp_ingest_api, cdp_query_api, refresh_token
  6. Note: Connected App Consumer Key + Secret

Usage:
    # Step 1: get OAuth token (run once, token lasts ~2 hrs)
    python generate_full_load.py --auth

    # Step 2: dry run — generate all 60 CSVs locally, no upload
    python generate_full_load.py --dry-run

    # Step 3: upload one month (test first)
    python generate_full_load.py --month 2020-07

    # Step 4: upload all 60 months sequentially
    python generate_full_load.py --all

Environment variables:
    DC_CLIENT_ID       Connected App Consumer Key
    DC_CLIENT_SECRET   Connected App Consumer Secret
    DC_USERNAME        your Salesforce / Data Cloud username
    DC_PASSWORD        your password
    DC_ORG_URL         your Data Cloud org URL, e.g. https://xyz.salesforce.com
    DC_STREAM_NAME     Data Stream API name, e.g. HPE_Forecast_Monthly
"""

import argparse
import csv
import io
import json
import os
import sys
import time
import requests

# ── Config ─────────────────────────────────────────────────────────────────────
DC_CLIENT_ID    = os.environ.get("DC_CLIENT_ID", "")
DC_CLIENT_SECRET= os.environ.get("DC_CLIENT_SECRET", "")
DC_USERNAME     = os.environ.get("DC_USERNAME", "vinayaksdesai777@gmail.com")
DC_PASSWORD     = os.environ.get("DC_PASSWORD", "")
DC_ORG_URL      = os.environ.get("DC_ORG_URL", "https://<your-dc-org>.salesforce.com")
DC_STREAM_NAME  = os.environ.get("DC_STREAM_NAME", "HPE_Forecast_Monthly")

TOKEN_CACHE     = ".dc_token_cache.json"

# Full load window
FULL_LOAD_START = (2020, 7)
FULL_LOAD_END   = (2025, 6)

VALID_CATEGORIES = ["SERVER","STORAGE","COMPUTE","NETWORKING",
                    "PRIVATE_CLOUD","SUPERCOMPUTING","AI"]
SUB_MAP = {
    "SERVER":"PROLIANT","STORAGE":"PRIMERA","COMPUTE":"SYNERGY",
    "NETWORKING":"ARUBA","PRIVATE_CLOUD":"GREENLAKE",
    "SUPERCOMPUTING":"CRAY_EX","AI":"AI_CLUSTER"
}
CHANNELS     = ["DIRECT","ONLINE","PARTNER","DISTRIBUTOR","VAR"]
REGIONS      = ["NORTH_AMERICA","EMEA","APJ","LATAM"]
CURRENCY_MAP = {"NORTH_AMERICA":"USD","EMEA":"EUR","APJ":"SGD","LATAM":"BRL"}
COUNTRY_MAP  = {
    "NORTH_AMERICA": ["US","CA"],
    "EMEA":          ["DE","FR","UK"],
    "APJ":           ["JP","IN","SG"],
    "LATAM":         ["BR","MX"],
}

# ── Month helpers ───────────────────────────────────────────────────────────────
def all_months() -> list[str]:
    months = []
    y, m = FULL_LOAD_START
    ey, em = FULL_LOAD_END
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1; y += 1
    return months

def rows_for_month(month_str: str) -> int:
    months = all_months()
    idx = months.index(month_str)
    # cycles between 5000-8000 in 500-row steps (7 steps)
    return 5000 + (idx % 7) * 500

# ── Data generation ─────────────────────────────────────────────────────────────
def generate_rows(month_str: str) -> list[dict]:
    y, m   = map(int, month_str.split("-"))
    f_date = f"{y:04d}-{m:02d}-01"
    n      = rows_for_month(month_str)
    rows   = []

    month_idx = all_months().index(month_str)

    for i in range(1, n + 1):
        # Business keys derive from position WITHIN the month, so each month
        # emits n distinct (product, location) pairs. Keying product to i % 500
        # and location to (i * 7) % 200 repeated the pair every
        # lcm(500, 200) = 1000 rows, so a 6,000-row month collapsed onto 1,000
        # real combinations. Salesforce has no unique constraint on these
        # fields, so it loaded silently and only showed up downstream as an
        # inflated fact count.
        # ceil(8000/200) = 40 product blocks x 200 locations covers the largest
        # month; the block offset rotates products across months so all 500 recur.
        prod_idx = (month_idx * 40 + (i - 1) // 200) % 500
        loc_idx  = (i - 1) % 200

        # Attributes are keyed to those same indexes, so a product always
        # carries the same category and a location always sits in the same
        # region/country. Keying them to i directly made them properties of the
        # row: i % 500 and i % 7 are coprime, so every product cycled through
        # all 7 categories and the Gold dimension churned a new SCD2 version on
        # every load.

        region   = REGIONS[loc_idx % 4]
        currency = CURRENCY_MAP[region]
        country  = COUNTRY_MAP[region][loc_idx % len(COUNTRY_MAP[region])]
        category = VALID_CATEGORIES[prod_idx % len(VALID_CATEGORIES)]
        sub_cat  = SUB_MAP[category]

        if i % 100 == 0: category, sub_cat = "HARDWARE", "LEGACY"
        elif i % 100 == 1: category, sub_cat = "UNKNOWN", "NA"
        if i % 67  == 0: currency = "XYZ"

        qty     = (i * 11 % 9800) + 100
        revenue = qty * ((i * 31 % 450) + 50)

        rows.append({
            "product_id":     f"HPE-PROD-{prod_idx + 1:04d}",
            "location_id":    f"LOC-{loc_idx + 1:03d}",
            "forecast_date":  f_date,
            "forecast_qty":   str(qty),
            "revenue_amount": str(revenue),
            "customer_id":    f"CUST-{(i * 13 % 50000) + 10000:05d}",
            "channel":        CHANNELS[i % 5],
            "category":       category,
            "sub_category":   sub_cat,
            "region":         region,
            "country":        country,
            "currency_code":  currency,
            "uom":            "UNIT",
            "period_type":    "MONTHLY",
            "fiscal_period":  month_str,
        })
    return rows

def rows_to_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    w   = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator="\r\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")

# ── OAuth: username-password flow for Data Cloud ────────────────────────────────
def get_token() -> tuple[str, str]:
    """Returns (access_token, instance_url). Caches to disk."""
    if os.path.exists(TOKEN_CACHE):
        with open(TOKEN_CACHE) as f:
            cached = json.load(f)
        if cached.get("expires_at", 0) > time.time() + 60:
            print("  Using cached token.")
            return cached["access_token"], cached["instance_url"]

    resp = requests.post(f"{DC_ORG_URL}/services/oauth2/token", data={
        "grant_type":    "password",
        "client_id":     DC_CLIENT_ID,
        "client_secret": DC_CLIENT_SECRET,
        "username":      DC_USERNAME,
        "password":      DC_PASSWORD,
    })
    resp.raise_for_status()
    data = resp.json()
    token       = data["access_token"]
    instance    = data["instance_url"]

    with open(TOKEN_CACHE, "w") as f:
        json.dump({
            "access_token": token,
            "instance_url": instance,
            "expires_at":   time.time() + 7000,  # ~2 hrs
        }, f)
    print(f"  Token obtained. Instance: {instance}")
    return token, instance

# ── Data Cloud Ingestion API ────────────────────────────────────────────────────
def ingest_batch(token: str, instance: str, rows: list[dict]) -> dict:
    """
    POST to /api/v1/ingest/jobs  (Bulk Ingestion Job)
    Docs: Salesforce Data Cloud Developer Guide > Ingestion API
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }
    base = f"{instance}/api/v1/ingest/jobs"

    # 1. Create job
    job = requests.post(base, headers=headers, json={
        "object":      DC_STREAM_NAME,
        "sourceName":  DC_STREAM_NAME,
        "operation":   "upsert",
    })
    job.raise_for_status()
    job_id = job.json()["id"]
    print(f"    Ingestion job: {job_id}")

    # 2. Upload CSV batch
    csv_bytes = rows_to_csv(rows)
    upload = requests.put(
        f"{base}/{job_id}/batches",
        headers={**headers, "Content-Type": "text/csv"},
        data=csv_bytes,
    )
    upload.raise_for_status()
    print(f"    Batch uploaded ({len(csv_bytes):,} bytes)")

    # 3. Close / commit job
    close = requests.patch(
        f"{base}/{job_id}",
        headers=headers,
        json={"state": "UploadComplete"},
    )
    close.raise_for_status()

    # 4. Poll for completion
    for attempt in range(40):
        time.sleep(6)
        status = requests.get(f"{base}/{job_id}", headers=headers).json()
        state  = status.get("state", "")
        print(f"    [{attempt+1}] state={state} "
              f"processed={status.get('recordsProcessed', '?')} "
              f"failed={status.get('recordsFailed', '?')}")
        if state in ("Complete", "Failed", "Aborted"):
            break

    return status

# ── Main ────────────────────────────────────────────────────────────────────────
def process_month(month_str: str, token: str, instance: str, dry_run: bool):
    rows = generate_rows(month_str)
    print(f"  [{month_str}] {len(rows):,} rows  (forecast_date = {month_str}-01)")

    # Always save CSV locally
    out_path = f"salesforce/data/fullload_{month_str}.csv"
    with open(out_path, "wb") as f:
        f.write(rows_to_csv(rows))
    print(f"    CSV → {out_path}")

    if dry_run:
        return len(rows), 0

    status = ingest_batch(token, instance, rows)
    ok   = status.get("recordsProcessed", 0)
    fail = status.get("recordsFailed", 0)
    return ok, fail


def main():
    parser = argparse.ArgumentParser(description="Data Cloud Full Load — HPE Forecast Monthly")
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--auth",    action="store_true", help="Obtain + cache OAuth token")
    group.add_argument("--month",   metavar="YYYY-MM",   help="Upload a single month")
    group.add_argument("--all",     action="store_true", help="Upload all 60 months")
    group.add_argument("--dry-run", action="store_true", help="Generate CSVs only, no upload")
    args = parser.parse_args()

    months     = all_months()
    total_rows = sum(rows_for_month(m) for m in months)
    print(f"Window : {months[0]} → {months[-1]}  ({len(months)} months, ~{total_rows:,} rows total)")
    print(f"Stream : {DC_STREAM_NAME}  |  Org: {DC_ORG_URL}")

    if args.auth:
        token, instance = get_token()
        print(f"Token cached to {TOKEN_CACHE}")
        return

    if args.dry_run:
        for m in months:
            process_month(m, None, None, dry_run=True)
        print(f"\nDone (dry run). {len(months)} CSVs written to salesforce/data/")
        return

    if not DC_CLIENT_ID or not DC_PASSWORD:
        print("Set DC_CLIENT_ID, DC_CLIENT_SECRET, DC_PASSWORD env vars.")
        sys.exit(1)

    token, instance = get_token()

    if args.month:
        if args.month not in months:
            print(f"Month {args.month} not in window {months[0]}–{months[-1]}")
            sys.exit(1)
        ok, fail = process_month(args.month, token, instance, dry_run=False)
        print(f"\nDone. processed={ok:,} failed={fail:,}")

    elif args.all:
        total_ok = total_fail = 0
        for m in months:
            ok, fail = process_month(m, token, instance, dry_run=False)
            total_ok   += ok
            total_fail += fail
        print(f"\nAll done. Total processed={total_ok:,} failed={total_fail:,}")


if __name__ == "__main__":
    main()
