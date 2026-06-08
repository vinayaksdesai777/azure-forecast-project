-- ============================================================
-- Aggregation Views for Gold Layer Monitoring
-- ============================================================

-- Daily load summary
CREATE OR ALTER VIEW audit.vw_daily_load_summary AS
SELECT
    CAST(job_start_ts AS DATE) AS load_date,
    data_layer,
    COUNT(*) AS total_jobs,
    SUM(CASE WHEN job_status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN job_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    SUM(source_row_count) AS total_source_rows,
    SUM(target_row_count) AS total_target_rows,
    SUM(error_record_count) AS total_error_rows,
    AVG(job_duration_sec) AS avg_duration_sec
FROM audit.pipeline_audit
GROUP BY CAST(job_start_ts AS DATE), data_layer;
GO

-- Data quality summary
CREATE OR ALTER VIEW audit.vw_dq_summary AS
SELECT
    CAST(created_ts AS DATE) AS check_date,
    table_name,
    check_type,
    SUM(records_checked) AS total_checked,
    SUM(records_failed) AS total_failed,
    CAST(
        CASE WHEN SUM(records_checked) > 0
            THEN (1.0 - (CAST(SUM(records_failed) AS FLOAT) / SUM(records_checked))) * 100
            ELSE 100
        END AS DECIMAL(5,2)
    ) AS pass_rate_pct
FROM audit.data_quality_log
GROUP BY CAST(created_ts AS DATE), table_name, check_type;
GO

-- Aggregated audit report (KPI metrics summary from Gold layer)
CREATE OR ALTER VIEW audit.vw_aggregated_audit AS
SELECT
    pa.batch_id,
    pm.data_subject,
    pa.object_name AS table_name,
    pa.data_layer,
    pa.source_row_count,
    pa.target_row_count,
    pa.error_record_count,
    pa.job_duration_sec,
    pa.job_status,
    pa.file_name,
    pa.job_start_ts,
    pa.job_end_ts,
    pm.metric_columns AS keyfigures
FROM audit.pipeline_audit pa
LEFT JOIN audit.pipeline_metadata pm
    ON pa.source_system = pm.source_system
WHERE pa.data_layer = 'GOLD';
GO
