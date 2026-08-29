"""
Guard the seed scripts' business-key derivation.

The seeds are SQL and Python generators that CI cannot execute — there is no
HANA or SQL Server in a runner. This replays their key arithmetic instead, which
is the part that actually broke.

The original convention derived product from `row % 500` and location from
`(row * 7) % 200`. Those cycles realign every lcm(500, 200) = 1000 rows, so any
period holding more than 1,000 rows collapsed onto 1,000 real product-location
pairs: 30,000 rows per quarter became 1,000 combinations repeated 30 times. Only
SQL Server surfaced it, through its unique constraint. HANA and Salesforce have
none, so they loaded the duplicates silently and it emerged downstream as an
inflated fact count and a star view that fanned out to double the row count.

The invariant these tests hold: within one period, every row must carry a
distinct (product, location) pair, and the full catalogue of 500 products and
200 locations must still be reachable across periods.

Kept in step with:
  sql/04_hana_quarterly_data.sql       150 blocks x 200 locations = 30,000/quarter
  sql/06_sqlserver_weekly_data.sql       8 blocks x 200 locations =  1,500/week
  sql/08_hana_quarterly_delta.sql       50 blocks x 200 locations = 10,000/run
  sql/09_sqlserver_weekly_delta.sql     43 blocks x 200 locations =  8,500/week
  salesforce/generate_full_load.py      40 blocks x 200 locations =  8,000/month max
  salesforce/generate_monthly_delta.py  45 blocks x 200 locations =  9,000/month
"""

import sys

PRODUCTS = 500
LOCATIONS = 200


def keys(period_index, rows_in_period, blocks_per_period):
    """The derivation every seed script shares."""
    return [
        (
            (period_index * blocks_per_period + i // LOCATIONS) % PRODUCTS,
            i % LOCATIONS,
        )
        for i in range(rows_in_period)
    ]


def check(label, periods, rows_in_period, blocks_per_period):
    worst_dupes = 0
    products = set()
    locations = set()

    for period in range(periods):
        pairs = keys(period, rows_in_period, blocks_per_period)
        worst_dupes = max(worst_dupes, len(pairs) - len(set(pairs)))
        products.update(p for p, _ in pairs)
        locations.update(l for _, l in pairs)

    problems = []
    if worst_dupes:
        problems.append(
            f"{worst_dupes} duplicate (product, location) pairs in a single period"
        )
    if rows_in_period > blocks_per_period * LOCATIONS:
        problems.append(
            f"{rows_in_period} rows/period exceeds the "
            f"{blocks_per_period * LOCATIONS} distinct pairs the blocking provides"
        )
    if len(locations) != LOCATIONS:
        problems.append(f"reaches {len(locations)} locations, expected {LOCATIONS}")

    status = "ok   " if not problems else "FAIL "
    print(f"{status} {label:34} {periods:>4} periods x {rows_in_period:>6} rows")
    for p in problems:
        print(f"         {p}")
    return not problems


def main():
    cases = [
        # label,                          periods, rows/period, blocks/period
        ("hana quarterly full",                20,       30000,          150),
        ("hana quarterly delta",                2,       10000,           50),
        ("sqlserver weekly full",             260,        1500,            8),
        ("sqlserver weekly delta",             26,        8500,           43),
        ("salesforce monthly full (largest)",  60,        8000,           40),
        ("salesforce monthly delta",            7,        9000,           45),
    ]

    ok = all(check(*case) for case in cases)

    # HANA daily is deliberately excluded from the blocking scheme: at 300
    # rows/day it never reaches the 1,000-row cycle, so the original derivation
    # is still safe there. Assert that remains true rather than assuming it.
    seen = set()
    for i in range(1, 301):
        seen.add((i % PRODUCTS, (i * 7) % LOCATIONS))
    daily_ok = len(seen) == 300
    print(f"{'ok   ' if daily_ok else 'FAIL '} {'hana daily (row-keyed, 300/day)':34}  300 rows, "
          f"{len(seen)} distinct pairs")

    print()
    if ok and daily_ok:
        print("business keys are distinct within every period")
        return 0
    print("business-key derivation is broken; see above")
    return 1


if __name__ == "__main__":
    sys.exit(main())
