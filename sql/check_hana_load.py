"""
Report the state of the SAP HANA source tables.

Confirms whether the full loads (04_hana_quarterly_data.sql,
05_hana_daily_data.sql) have run and whether the corrected business keys took
effect. Expected after a clean seed:

    FORECAST_DAILY      ~547,800 rows + 64,600 delta = ~612,400
    FORECAST_QUARTERLY   600,000 rows
    both: 500 distinct products, 200 distinct locations, 0 duplicate keys

Usage (password is never stored in the repo):
    $env:HANA_PASSWORD="<DBADMIN password>"
    python sql/check_hana_load.py
"""

import os
import sys

try:
    from hdbcli import dbapi
except ImportError:
    sys.exit("hdbcli is required: pip install hdbcli")

HOST = "c1ad1e48-607f-4bfa-983b-80f69fd2a68a.hna1.prod-us10.hanacloud.ondemand.com"
PORT = 443
USER = os.environ.get("HANA_USER", "DBADMIN")
PASSWORD = os.environ.get("HANA_PASSWORD", "")

TABLES = ["FORECAST_DAILY", "FORECAST_QUARTERLY"]


def main() -> None:
    if not PASSWORD:
        sys.exit("Set HANA_PASSWORD (and optionally HANA_USER) first.")

    conn = dbapi.connect(address=HOST, port=PORT, user=USER,
                         password=PASSWORD, encrypt=True, sslValidateCertificate=True)
    cur = conn.cursor()

    for table in TABLES:
        print(f"\n=== O9_SOURCE.{table} ===")
        try:
            cur.execute(f'''
                SELECT COUNT(*)                     AS TOTAL,
                       COUNT(DISTINCT PRODUCT_ID)   AS PRODS,
                       COUNT(DISTINCT LOCATION_ID)  AS LOCS,
                       MIN(FORECAST_DATE)           AS MIN_DT,
                       MAX(FORECAST_DATE)           AS MAX_DT
                FROM "O9_SOURCE"."{table}"
            ''')
            total, prods, locs, min_dt, max_dt = cur.fetchone()
            print(f"  rows       : {total:,}")
            print(f"  products   : {prods}   (expect 500)")
            print(f"  locations  : {locs}   (expect 200)")
            print(f"  date range : {min_dt} .. {max_dt}")

            # Duplicate business keys are the thing the key fix was meant to remove.
            cur.execute(f'''
                SELECT COUNT(*) FROM (
                    SELECT PRODUCT_ID, LOCATION_ID, FORECAST_DATE
                    FROM "O9_SOURCE"."{table}"
                    WHERE PRODUCT_ID IS NOT NULL
                    GROUP BY PRODUCT_ID, LOCATION_ID, FORECAST_DATE
                    HAVING COUNT(*) > 1
                )
            ''')
            dupes = cur.fetchone()[0]
            print(f"  duplicate keys: {dupes}   (expect 0)")
        except Exception as exc:
            print(f"  ERROR: {exc}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
