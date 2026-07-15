"""
Salesforce Monthly Forecast — FULL LOAD Generator
Generates 60 months of data (Jul 2020 - Jun 2025) and uploads via Bulk API 2.0.
Target: 5,000-8,000 rows/month (~390,000 rows total across 60 months).

Because Salesforce Developer Edition has a 5MB storage cap, this script
uploads in monthly batches so you can stop/resume and monitor storage usage.

Usage:
    # Dry run — generates CSVs only, no upload
    python generate_full_load.py --dry-run

    # Upload one month at a time (recommended — monitor storage between runs)
    python generate_full_load.py --month 2020-07
    python generate_full_load.py --month 2020-08
    ...

    # Upload all 60 months sequentially (unattended — watch storage limit)
    python generate_full_load.py --all

Environment variables required for upload:
    SF_PASSWORD   your Salesforce password
    SF_TOKEN      your Salesforce security token
"""

import argparse
import csv
import io
import json
import os
import sys
import time
import requests
from datetime import date
from calendar import monthrange

# ── Config ────────────────────────────────────────────────────────────────────
SF_USERNAME       = "vinayaksdesai777@gmail.com"
SF_PASSWORD       = os.environ.get("SF_PASSWORD", "")
SF_SECURITY_TOKEN = os.environ.get("SF_TOKEN", "")
SF_LOGIN_URL      = "https://login.salesforce.com"
OBJECT_API_NAME   = "Forecast__c"

# Full load window
FULL_LOAD_START = (2020, 7)   # Jul 2020
FULL_LOAD_END   = (2025, 6)   # Jun 2025 inclusive

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

# ── Month list ────────────────────────────────────────────────────────────────
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

# ── Row count per month: vary between 5000-8000 ───────────────────────────────
def rows_for_month(month_str: str) -> int:
    # deterministic variation based on month index so reruns are stable
    months = all_months()
    idx = months.index(month_str)
    # cycle: 5000, 5500, 6000, 6500, 7000, 7500, 8000, back to 5000
    return 5000 + (idx % 7) * 500

# ── Data generation ───────────────────────────────────────────────────────────
def generate_rows(month_str: str) -> list[dict]:
    y, m = map(int, month_str.split("-"))
    forecast_date = date(y, m, 1)
    n = rows_for_month(month_str)
    rows = []

    for i in range(1, n + 1):
        region      = REGIONS[i % 4]
        currency    = CURRENCY_MAP[region]
        country     = COUNTRY_MAP[region][i % len(COUNTRY_MAP[region])]
        category    = VALID_CATEGORIES[i % len(VALID_CATEGORIES)]
        sub_cat     = SUB_MAP[category]

        # ~2% dirty category
        if i % 100 == 0:
            category, sub_cat = "HARDWARE", "LEGACY"
        elif i % 100 == 1:
            category, sub_cat = "UNKNOWN", "NA"

        # ~1.5% dirty currency
        if i % 67 == 0:
            currency = "XYZ"

        qty     = (i * 11 % 9800) + 100
        revenue = qty * ((i * 31 % 450) + 50)

        rows.append({
            "Name":              f"FL-{month_str}-{i:05d}",
            "Product_Id__c":     f"HPE-PROD-{(i % 500) + 1:04d}",
            "Location_Id__c":    f"LOC-{(i * 7 % 200) + 1:03d}",
            "Forecast_Date__c":  str(forecast_date),
            "Forecast_Qty__c":   str(qty),
            "Revenue_Amount__c": str(revenue),
            "Customer_Id__c":    f"CUST-{(i * 13 % 50000) + 10000:05d}",
            "Channel__c":        CHANNELS[i % 5],
            "Category__c":       category,
            "Sub_Category__c":   sub_cat,
            "Region__c":         region,
            "Country__c":        country,
            "Currency_Code__c":  currency,
            "UOM__c":            "UNIT",
            "Period_Type__c":    "MONTHLY",
            "Fiscal_Period__c":  month_str,
        })
    return rows

def rows_to_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")

# ── Salesforce auth ───────────────────────────────────────────────────────────
def sf_login(username, password, token, login_url):
    resp = requests.post(f"{login_url}/services/Soap/u/58.0", headers={
        "Content-Type": "text/xml; charset=UTF-8", "SOAPAction": "login"
    }, data=f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:urn="urn:partner.soap.sforce.com">
  <soapenv:Body><urn:login>
    <urn:username>{username}</urn:username>
    <urn:password>{password}{token}</urn:password>
  </urn:login></soapenv:Body>
</soapenv:Envelope>""")
    import xml.etree.ElementTree as ET
    ns   = {"p": "urn:partner.soap.sforce.com"}
    root = ET.fromstring(resp.text)
    sid  = root.findtext(".//p:sessionId", namespaces=ns)
    url  = root.findtext(".//p:serverUrl",  namespaces=ns)
    return sid, url.split("/services")[0]

def bulk_insert(session_id, instance, csv_bytes) -> dict:
    base    = f"{instance}/services/data/v58.0/jobs/ingest"
    headers = {"Authorization": f"Bearer {session_id}", "Content-Type": "application/json"}

    job    = requests.post(base, headers=headers, json={
        "object": OBJECT_API_NAME, "operation": "insert",
        "contentType": "CSV", "lineEnding": "CRLF"
    }).json()
    job_id = job["id"]

    requests.put(f"{base}/{job_id}/batches",
        headers={**headers, "Content-Type": "text/csv"}, data=csv_bytes)
    requests.patch(f"{base}/{job_id}", headers=headers,
        json={"state": "UploadComplete"})

    for _ in range(30):
        time.sleep(8)
        status = requests.get(f"{base}/{job_id}", headers=headers).json()
        state  = status.get("state", "")
        if state in ("JobComplete", "Failed", "Aborted"):
            break
    return status

# ── Main ──────────────────────────────────────────────────────────────────────
def process_month(month_str, session_id, instance, dry_run):
    rows      = generate_rows(month_str)
    csv_bytes = rows_to_csv(rows)
    out_path  = f"salesforce/data/fullload_{month_str}.csv"
    with open(out_path, "wb") as f:
        f.write(csv_bytes)
    print(f"  [{month_str}] {len(rows):,} rows → {out_path}")

    if not dry_run:
        result = bulk_insert(session_id, instance, csv_bytes)
        proc   = result.get("numberRecordsProcessed", 0)
        fail   = result.get("numberRecordsFailed", 0)
        print(f"  [{month_str}] state={result.get('state')} processed={proc:,} failed={fail:,}")
        return proc, fail
    return len(rows), 0

def main():
    parser = argparse.ArgumentParser()
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--month",   help="Single month, e.g. 2020-07")
    group.add_argument("--all",     action="store_true", help="Upload all 60 months")
    group.add_argument("--dry-run", action="store_true", help="Generate CSVs only")
    args = parser.parse_args()

    months = all_months()
    total_rows = sum(rows_for_month(m) for m in months)
    print(f"Full load window: {months[0]} to {months[-1]} ({len(months)} months)")
    print(f"Total rows across all months: {total_rows:,}")

    session_id = instance = None
    dry_run = args.dry_run or False

    if not dry_run:
        if not SF_PASSWORD or not SF_SECURITY_TOKEN:
            print("Set SF_PASSWORD and SF_TOKEN env vars.")
            sys.exit(1)
        print("Logging into Salesforce...")
        session_id, instance = sf_login(
            SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN, SF_LOGIN_URL)
        print(f"  Instance: {instance}")

    if args.dry_run:
        for m in months:
            process_month(m, None, None, dry_run=True)
    elif args.month:
        if args.month not in months:
            print(f"Month {args.month} not in full load window {months[0]}–{months[-1]}")
            sys.exit(1)
        process_month(args.month, session_id, instance, dry_run=False)
    elif args.all:
        total_proc = total_fail = 0
        for m in months:
            p, f = process_month(m, session_id, instance, dry_run=False)
            total_proc += p; total_fail += f
        print(f"\nDone. Total processed={total_proc:,} failed={total_fail:,}")

if __name__ == "__main__":
    main()
