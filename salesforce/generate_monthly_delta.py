"""
Salesforce Monthly Forecast — Incremental Delta Generator
Generates one month of delta data (~9,000 rows) and uploads via Bulk API 2.0.

Usage:
    python generate_monthly_delta.py --month 2026-01
    python generate_monthly_delta.py --month 2026-02
    ...up to 2026-07

Each run = one ADF monthly trigger simulation.
Full load was Jan-Dec 2025 (84,000 rows). Deltas are Jan-Jul 2026.
"""

import argparse
import csv
import io
import json
import os
import sys
import time
import requests
from datetime import date, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
SF_USERNAME      = "vinayaksdesai777@gmail.com"
SF_PASSWORD      = os.environ.get("SF_PASSWORD", "")
SF_SECURITY_TOKEN= os.environ.get("SF_TOKEN", "")
SF_LOGIN_URL     = "https://login.salesforce.com"
ROWS_PER_MONTH   = 9000
OBJECT_API_NAME  = "Forecast__c"

VALID_CATEGORIES = ["SERVER","STORAGE","COMPUTE","NETWORKING",
                    "PRIVATE_CLOUD","SUPERCOMPUTING","AI"]
CHANNELS         = ["DIRECT","ONLINE","PARTNER","DISTRIBUTOR","VAR"]
REGIONS          = ["NORTH_AMERICA","EMEA","APJ","LATAM"]
COUNTRY_MAP      = {
    "NORTH_AMERICA": ["US","CA"],
    "EMEA":          ["DE","FR","UK"],
    "APJ":           ["JP","IN","SG"],
    "LATAM":         ["BR","MX"],
}
CURRENCY_MAP     = {"NORTH_AMERICA":"USD","EMEA":"EUR","APJ":"SGD","LATAM":"BRL"}

# ── Data generation ────────────────────────────────────────────────────────────
def first_day_of_month(ym: str) -> date:
    y, m = map(int, ym.split("-"))
    return date(y, m, 1)

def generate_delta_rows(month_str: str) -> list[dict]:
    forecast_date = first_day_of_month(month_str)
    fiscal_period = month_str   # e.g. "2026-01"
    rows = []

    for i in range(1, ROWS_PER_MONTH + 1):
        # Keyed to the same expressions that build product_id / location_id
        # below, so a product keeps its category and a location keeps its
        # region and country across every load. Keying them to i directly made
        # them properties of the row, which churned the Gold SCD2 dimensions.
        prod_idx = i % 500
        loc_idx  = (i * 7) % 200

        region   = REGIONS[loc_idx % 4]
        currency = CURRENCY_MAP[region]
        country  = COUNTRY_MAP[region][loc_idx % len(COUNTRY_MAP[region])]

        # ~2% dirty currency
        if i % 67 == 0:
            currency = "XYZ"

        # base category
        cat_idx  = prod_idx % len(VALID_CATEGORIES)
        category = VALID_CATEGORIES[cat_idx]
        sub_map  = {
            "SERVER":"PROLIANT","STORAGE":"PRIMERA","COMPUTE":"SYNERGY",
            "NETWORKING":"ARUBA","PRIVATE_CLOUD":"GREENLAKE",
            "SUPERCOMPUTING":"CRAY_EX","AI":"AI_CLUSTER"
        }
        sub_category = sub_map[category]

        # ~2% dirty category
        if i % 100 == 0:
            category, sub_category = "HARDWARE", "LEGACY"
        elif i % 100 == 1:
            category, sub_category = "UNKNOWN", "NA"

        product_id  = f"HPE-PROD-{(i % 500) + 1:04d}"
        location_id = f"LOC-{(i * 7 % 200) + 1:03d}"
        customer_id = f"CUST-{(i * 13 % 50000) + 10000:05d}"
        qty         = (i * 11 % 9800) + 100
        revenue     = qty * ((i * 31 % 450) + 50)

        rows.append({
            "Name":              f"DELTA-{month_str}-{i:05d}",
            "Product_Id__c":     product_id,
            "Location_Id__c":    location_id,
            "Forecast_Date__c":  str(forecast_date),
            "Forecast_Qty__c":   str(qty),
            "Revenue_Amount__c": str(revenue),
            "Customer_Id__c":    customer_id,
            "Channel__c":        CHANNELS[i % 5],
            "Category__c":       category,
            "Sub_Category__c":   sub_category,
            "Region__c":         region,
            "Country__c":        country,
            "Currency_Code__c":  currency,
            "UOM__c":            "UNIT",
            "Period_Type__c":    "MONTHLY",
            "Fiscal_Period__c":  fiscal_period,
        })

    return rows

# ── CSV builder ───────────────────────────────────────────────────────────────
def rows_to_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")

# ── Salesforce Bulk API 2.0 ───────────────────────────────────────────────────
def sf_login(username, password, token, login_url):
    resp = requests.post(f"{login_url}/services/Soap/u/58.0", headers={
        "Content-Type": "text/xml; charset=UTF-8",
        "SOAPAction": "login"
    }, data=f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:urn="urn:partner.soap.sforce.com">
  <soapenv:Body><urn:login>
    <urn:username>{username}</urn:username>
    <urn:password>{password}{token}</urn:password>
  </urn:login></soapenv:Body>
</soapenv:Envelope>""")
    import xml.etree.ElementTree as ET
    ns = {"p": "urn:partner.soap.sforce.com"}
    root = ET.fromstring(resp.text)
    session_id = root.findtext(".//p:sessionId", namespaces=ns)
    server_url  = root.findtext(".//p:serverUrl", namespaces=ns)
    instance    = server_url.split("/services")[0]
    return session_id, instance

def bulk_insert(session_id, instance, object_name, csv_bytes):
    base    = f"{instance}/services/data/v58.0/jobs/ingest"
    headers = {"Authorization": f"Bearer {session_id}", "Content-Type": "application/json"}

    # 1. Create job
    job = requests.post(base, headers=headers, json={
        "object": object_name, "operation": "insert",
        "contentType": "CSV", "lineEnding": "CRLF"
    }).json()
    job_id = job["id"]
    print(f"  Job created: {job_id}")

    # 2. Upload CSV
    requests.put(f"{base}/{job_id}/batches",
        headers={**headers, "Content-Type": "text/csv"}, data=csv_bytes)
    print(f"  CSV uploaded ({len(csv_bytes):,} bytes)")

    # 3. Close job
    requests.patch(f"{base}/{job_id}",
        headers=headers, json={"state": "UploadComplete"})

    # 4. Poll until done
    for _ in range(30):
        time.sleep(10)
        status = requests.get(f"{base}/{job_id}", headers=headers).json()
        state  = status.get("state", "")
        print(f"  Status: {state} | success={status.get('numberRecordsProcessed',0)} "
              f"failed={status.get('numberRecordsFailed',0)}")
        if state in ("JobComplete", "Failed", "Aborted"):
            break

    return status

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True,
        help="Month to generate, e.g. 2026-01 through 2026-07")
    parser.add_argument("--dry-run", action="store_true",
        help="Generate CSV only, skip Salesforce upload")
    args = parser.parse_args()

    print(f"\n=== Monthly Delta: {args.month} ===")
    rows = generate_delta_rows(args.month)
    print(f"Generated {len(rows):,} rows for Forecast_Date = {rows[0]['Forecast_Date__c']}")

    csv_bytes = rows_to_csv(rows)
    out_path  = f"salesforce/data/delta_{args.month}.csv"
    with open(out_path, "wb") as f:
        f.write(csv_bytes)
    print(f"CSV saved: {out_path}")

    if args.dry_run:
        print("--dry-run: skipping Salesforce upload.")
        return

    if not SF_PASSWORD or not SF_SECURITY_TOKEN:
        print("Set SF_PASSWORD and SF_TOKEN env vars to upload.")
        sys.exit(1)

    print("Logging into Salesforce...")
    session_id, instance = sf_login(SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN, SF_LOGIN_URL)
    print(f"  Instance: {instance}")

    print("Starting Bulk API 2.0 insert job...")
    result = bulk_insert(session_id, instance, OBJECT_API_NAME, csv_bytes)
    print(f"\nDone. Final state: {result.get('state')}")
    print(f"  Processed: {result.get('numberRecordsProcessed',0):,}")
    print(f"  Failed:    {result.get('numberRecordsFailed',0):,}")

if __name__ == "__main__":
    main()
