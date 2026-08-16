"""
Convert the generated monthly forecast CSVs to landing Parquet.

Why this exists
---------------
The monthly subject's full load is seeded straight into the landing container
rather than through Salesforce. Developer Edition tops out near 84,000 records,
so the full 60-month / 387,000-row history cannot live in Forecast__c. Landing
the history directly keeps all 60 months while the ongoing monthly refresh
(~84,000 rows) still flows through the real Salesforce -> ADF extract.

Output contract
---------------
Column names carry the ``__c`` suffix that the ADF Salesforce connector emits,
because ``01_landing_to_silver.py`` maps exactly those names for
``source_system == "SALESFORCE"`` (see ``_SF_COL_MAP``). Files landed here are
therefore indistinguishable from a real extract, and one silver code path
serves both.

Every column is written as a string. The silver notebook casts everything to
string on read anyway ("schema-on-read"), and TypeCastTransform decides the
real types downstream. Writing strings here keeps the two paths identical.

Usage
-----
    python salesforce/csv_to_landing_parquet.py --out-dir ./landing_out
    python salesforce/csv_to_landing_parquet.py --out-dir ./landing_out --month 2020-07
"""

import argparse
import csv
import glob
import os
import sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    sys.exit("pyarrow is required: pip install pyarrow")

# CSV header -> landing Parquet column, matching _SF_COL_MAP in
# databricks/notebooks/01_landing_to_silver.py.
COLUMN_MAP = {
    "product_id":     "Product_Id__c",
    "location_id":    "Location_Id__c",
    "forecast_date":  "Forecast_Date__c",
    "forecast_qty":   "Forecast_Qty__c",
    "revenue_amount": "Revenue_Amount__c",
    "customer_id":    "Customer_Id__c",
    "channel":        "Channel__c",
    "category":       "Category__c",
    "sub_category":   "Sub_Category__c",
    "region":         "Region__c",
    "country":        "Country__c",
    "currency_code":  "Currency_Code__c",
    "uom":            "UOM__c",
    "period_type":    "Period_Type__c",
    "fiscal_period":  "Fiscal_Period__c",
}


def convert(csv_path: str, out_dir: str) -> tuple[str, int]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        raise ValueError(f"{csv_path} has no data rows")

    missing = set(COLUMN_MAP) - set(rows[0])
    if missing:
        raise ValueError(f"{csv_path} missing expected columns: {sorted(missing)}")

    # Empty strings become NULL so the seeded dirty rows (NULL product_id)
    # survive as nulls rather than turning into the literal "".
    columns = {
        target: pa.array([(r[source] if r[source] != "" else None) for r in rows],
                         type=pa.string())
        for source, target in COLUMN_MAP.items()
    }
    table = pa.table(columns)

    base = os.path.basename(csv_path).replace(".csv", ".parquet")
    out_path = os.path.join(out_dir, base)
    pq.write_table(table, out_path, compression="snappy")
    return out_path, len(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True,
                    help="Local directory for the Parquet files")
    ap.add_argument("--month", help="Convert a single month, e.g. 2020-07")
    ap.add_argument("--data-dir", default="salesforce/data",
                    help="Directory holding fullload_*.csv")
    args = ap.parse_args()

    pattern = f"fullload_{args.month}.csv" if args.month else "fullload_*.csv"
    files = sorted(glob.glob(os.path.join(args.data_dir, pattern)))
    if not files:
        sys.exit(f"No CSVs matched {os.path.join(args.data_dir, pattern)}")

    os.makedirs(args.out_dir, exist_ok=True)

    total = 0
    for fp in files:
        out_path, n = convert(fp, args.out_dir)
        total += n
        print(f"  {os.path.basename(fp)} -> {os.path.basename(out_path)}  ({n:,} rows)")

    size_mb = sum(os.path.getsize(os.path.join(args.out_dir, f))
                  for f in os.listdir(args.out_dir)
                  if f.endswith(".parquet")) / 1024 / 1024
    print(f"\n{len(files)} file(s), {total:,} rows, {size_mb:.1f} MB in {args.out_dir}")


if __name__ == "__main__":
    main()
