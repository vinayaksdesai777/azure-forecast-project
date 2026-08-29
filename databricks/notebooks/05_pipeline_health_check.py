# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Pipeline Health Check
# MAGIC
# MAGIC The ADF metric alerts fire on `PipelineFailedRuns`, `ActivityFailedRuns`
# MAGIC and `TriggerFailedRuns`. None of them can see the failure that matters
# MAGIC most on a schedule: a run that **succeeds having loaded nothing**.
# MAGIC
# MAGIC The pipeline reaches that state deliberately — `Log No Source Files` when
# MAGIC landing is empty, and Silver's `NO_DATA` exit when the extract matched no
# MAGIC rows. Both are correct behaviour for a cycle with genuinely nothing new,
# MAGIC and both are also exactly what a broken watermark, a stopped source or a
# MAGIC misconfigured filter look like. ADF records SUCCESS either way.
# MAGIC
# MAGIC This notebook reads `audit.job_log` and raises when a subject looks
# MAGIC unhealthy, so the failure becomes visible through the alerting that
# MAGIC already exists rather than needing a second channel.
# MAGIC
# MAGIC Three checks per subject:
# MAGIC
# MAGIC | Check | Fails when |
# MAGIC |---|---|
# MAGIC | Freshness | no successful Silver load within the subject's SLA window |
# MAGIC | Volume | the newest load wrote zero rows AND zero updates |
# MAGIC | Errors | any FAILED row since the last successful load |
# MAGIC
# MAGIC Run it as a scheduled ADF activity after the medallion load, or on its
# MAGIC own trigger a few hours after the daily window closes.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime, timedelta

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("data_subject", "", "Subject to check (empty = all active)")
dbutils.widgets.text("fail_on_issues", "true", "Raise on findings (false = report only)")

subject_filter = dbutils.widgets.get("data_subject").strip()
fail_on_issues = dbutils.widgets.get("fail_on_issues").strip().lower() == "true"

# COMMAND ----------

# How long a subject may go without a successful load before it is stale:
# its cadence plus a grace period, NOT a multiple of the cadence.
#
# The first version used generous multiples (monthly = 35 days) on the reasoning
# that a whole missed cycle is what matters. Measured against real data that
# turned out to detect nothing useful: monthly and quarterly had both been 10
# days without a load and the check still reported zero findings. A monthly
# subject that fails on the 1st should not go unnoticed until the 5th of the
# following month.
#
# Grace is roughly one cadence period for the sub-daily subjects and 72h for
# the rest, which tolerates a single retried run without tolerating a silent
# outage.
#
# Be clear about the limit of this check. For the infrequent subjects it is
# necessarily weak: a monthly load legitimately goes ~31 days between runs, so
# any window that tolerates normal operation also tolerates a fortnight of
# silence. Measured here, monthly sat 239h stale and still read 29% of its
# window. Freshness alone cannot catch a missed monthly run promptly — what
# catches it is TriggerFailedRuns (the trigger did not fire) plus the volume
# check below (the run fired and moved nothing). This check is the backstop for
# the case where neither of those fires, not the primary signal.
#
# The honest fix for monthly and quarterly is to compare against the SCHEDULE
# rather than elapsed time: after the 1st of a month, assert a load exists with
# insert_time in that month. That needs the trigger cadence in pipeline_config,
# which it does not carry today.
SLA_HOURS = {
    "o9_forecast_daily":     36,       # 24h cadence + 12h
    "o9_forecast_weekly":    9 * 24,   # 7d  cadence + 2d
    "o9_forecast_monthly":   34 * 24,  # 31d cadence + 3d
    "o9_forecast_quarterly": 95 * 24,  # 92d cadence + 3d
}

# A load that is merely late is worth seeing before it breaches. Anything past
# this fraction of the SLA is reported as AGEING without failing the run, so a
# drift shows up in the log while there is still time to act.
WARN_FRACTION = 0.75

# COMMAND ----------

subjects_df = spark.sql(f"""
    SELECT data_subject
    FROM   hpe_catalog.audit.pipeline_config
    WHERE  is_active = true
      {f"AND data_subject = '{subject_filter}'" if subject_filter else ""}
""")
subjects = [r["data_subject"] for r in subjects_df.collect()]

if not subjects:
    raise ValueError(f"No active pipeline_config rows for filter={subject_filter!r}")

print(f"Checking {len(subjects)} subject(s): {', '.join(subjects)}")

# COMMAND ----------

findings = []
warnings = []
report = []

for subject in subjects:
    sla = SLA_HOURS.get(subject, 36)

    # Latest successful silver load. Silver is the right layer to judge: it is
    # the first that writes business rows, so a gold row count alone could look
    # healthy while nothing new arrived.
    latest = spark.sql(f"""
        SELECT insert_time, records_inserted, records_updated, error_records, batch_id
        FROM   hpe_catalog.audit.job_log
        WHERE  data_subject = '{subject}'
          AND  layer  = 'silver'
          AND  status = 'SUCCESS'
        ORDER  BY insert_time DESC
        LIMIT  1
    """).first()

    if latest is None:
        findings.append(f"{subject}: no successful silver load has ever been recorded")
        report.append((subject, None, None, None, "NEVER LOADED"))
        continue

    age_hours = (datetime.utcnow() - latest["insert_time"]).total_seconds() / 3600
    inserted = latest["records_inserted"] or 0
    updated = latest["records_updated"] or 0

    status = "ok"

    # 1. Freshness
    if age_hours > sla:
        findings.append(
            f"{subject}: last successful load was {age_hours:.1f}h ago, "
            f"beyond the {sla}h SLA"
        )
        status = "STALE"
    elif age_hours > sla * WARN_FRACTION:
        # Reported, not a finding: this does not fail the run.
        warnings.append(
            f"{subject}: {age_hours:.1f}h since the last load, "
            f"{100 * age_hours / sla:.0f}% of its {sla}h SLA"
        )
        status = "AGEING"

    # 2. Volume. Zero inserts AND zero updates means the run moved no data at
    #    all. Zero inserts with a non-zero update count is a normal restatement
    #    cycle, so both must be zero before this is worth reporting.
    if inserted == 0 and updated == 0:
        findings.append(
            f"{subject}: most recent successful load wrote nothing "
            f"(batch {latest['batch_id']}) — check the watermark, the source, "
            f"and whether landing held any files"
        )
        status = "EMPTY" if status == "ok" else status

    # 3. Failures since that load
    failures = spark.sql(f"""
        SELECT count(*) AS n
        FROM   hpe_catalog.audit.job_log
        WHERE  data_subject = '{subject}'
          AND  status <> 'SUCCESS'
          AND  insert_time > timestamp('{latest["insert_time"]}')
    """).first()["n"]

    if failures:
        findings.append(f"{subject}: {failures} failed job_log row(s) since the last success")
        status = "FAILURES" if status == "ok" else status

    report.append((subject, round(age_hours, 1), inserted, updated, status))

# COMMAND ----------

print(f"{'subject':26} {'age_h':>7} {'inserted':>10} {'updated':>9}  status")
print("-" * 68)
for subject, age, ins, upd, status in report:
    age_s = f"{age}" if age is not None else "-"
    ins_s = f"{ins:,}" if ins is not None else "-"
    upd_s = f"{upd:,}" if upd is not None else "-"
    print(f"{subject:26} {age_s:>7} {ins_s:>10} {upd_s:>9}  {status}")

# COMMAND ----------

if warnings:
    print()
    print("Ageing (not yet breaching):")
    for w in warnings:
        print(f"  {w}")

if findings:
    message = f"{len(findings)} pipeline health finding(s):\n  " + "\n  ".join(findings)
    print("\n" + message)
    if fail_on_issues:
        # Raising makes the ADF activity fail, so this surfaces through the
        # PipelineFailedRuns and ActivityFailedRuns alerts already in place
        # rather than needing its own notification channel.
        raise RuntimeError(message)
else:
    print("\nAll subjects fresh, non-empty, and free of failures since their last load.")

dbutils.notebook.exit(
    f'{{"findings":{len(findings)},"subjects_checked":{len(subjects)}}}'
)
