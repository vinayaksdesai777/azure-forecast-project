-- ============================================================
-- Azure SQL Database: Audit Tables
-- Centralized pipeline audit and metadata tracking
-- ============================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'audit')
BEGIN
    EXEC('CREATE SCHEMA audit');
END
GO

-- ============================================================
-- Pipeline Audit Table (tracks each layer load)
-- ============================================================
CREATE TABLE audit.pipeline_audit (
    audit_id            INT IDENTITY(1,1) PRIMARY KEY,
    batch_id            NVARCHAR(100) NOT NULL,
    application_id      NVARCHAR(200),
    object_name         NVARCHAR(200) NOT NULL,
    data_layer          NVARCHAR(50) NOT NULL,       -- BRONZE, SILVER, GOLD, AGGR_AUDIT
    job_start_ts        DATETIME2 NOT NULL,
    job_end_ts          DATETIME2,
    job_status          NVARCHAR(20) NOT NULL,       -- SUCCESS, FAILED, RUNNING
    source_row_count    BIGINT DEFAULT 0,
    target_row_count    BIGINT DEFAULT 0,
    error_record_count  BIGINT DEFAULT 0,
    job_duration_sec    INT,
    source_system       NVARCHAR(100),
    file_name           NVARCHAR(500),
    load_job_number     NVARCHAR(100),
    created_by          NVARCHAR(100),
    created_ts          DATETIME2 DEFAULT GETUTCDATE(),
    updated_ts          DATETIME2 DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_pipeline_audit_batch ON audit.pipeline_audit(batch_id);
CREATE INDEX IX_pipeline_audit_status ON audit.pipeline_audit(job_status, data_layer);
CREATE INDEX IX_pipeline_audit_object ON audit.pipeline_audit(object_name, job_start_ts);
GO

-- ============================================================
-- Data Quality Log Table
-- ============================================================
CREATE TABLE audit.data_quality_log (
    dq_id               INT IDENTITY(1,1) PRIMARY KEY,
    batch_id            NVARCHAR(100) NOT NULL,
    table_name          NVARCHAR(200) NOT NULL,
    check_type          NVARCHAR(50) NOT NULL,       -- NULL_CHECK, DUPLICATE_CHECK, FORMAT_CHECK
    column_name         NVARCHAR(200),
    records_checked     BIGINT DEFAULT 0,
    records_failed      BIGINT DEFAULT 0,
    check_status        NVARCHAR(20) NOT NULL,       -- PASSED, FAILED, WARNING
    error_message       NVARCHAR(MAX),
    created_ts          DATETIME2 DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_dq_log_batch ON audit.data_quality_log(batch_id);
GO

-- ============================================================
-- Pipeline Metadata Table (configuration per data subject)
-- ============================================================
CREATE TABLE audit.pipeline_metadata (
    metadata_id         INT IDENTITY(1,1) PRIMARY KEY,
    data_subject        NVARCHAR(100) NOT NULL,
    source_system       NVARCHAR(100) NOT NULL,
    frequency           NVARCHAR(20) NOT NULL DEFAULT 'daily',  -- daily, weekly, monthly, quarterly
    source_path         NVARCHAR(500),
    landing_container   NVARCHAR(200),
    bronze_table        NVARCHAR(200),
    silver_table        NVARCHAR(200),
    gold_table          NVARCHAR(200),
    file_format         NVARCHAR(20) DEFAULT 'CSV',
    delimiter           NVARCHAR(5) DEFAULT '|',
    has_header          BIT DEFAULT 1,
    null_check_columns  NVARCHAR(MAX),
    partition_column    NVARCHAR(200),
    apply_concat        BIT DEFAULT 0,
    group_columns       NVARCHAR(500),
    metric_columns      NVARCHAR(MAX),
    num_partitions      INT DEFAULT 8,
    is_active           BIT DEFAULT 1,
    created_ts          DATETIME2 DEFAULT GETUTCDATE(),
    updated_ts          DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- ============================================================
-- Insert sample metadata for the o9 data subject
-- ============================================================
-- Daily forecast: granular SKU-level, next 7-14 days
INSERT INTO audit.pipeline_metadata (
    data_subject, source_system, frequency, source_path, landing_container,
    bronze_table, silver_table, gold_table,
    file_format, delimiter, has_header,
    null_check_columns, partition_column, apply_concat,
    group_columns, metric_columns, num_partitions
) VALUES (
    'o9_forecast_daily', 'o9', 'daily', 'o9/daily/', 'landing',
    'bronze.o9_forecast_raw', 'silver.o9_forecast_ref', 'gold.o9_forecast_dmnsn',
    'CSV', '|', 1,
    'product_id,location_id,forecast_date', 'forecast_date', 1,
    'fl_nm', 'forecast_qty,revenue_amount', 8
);
GO

-- Weekly forecast: aggregated by week, next 4-12 weeks
INSERT INTO audit.pipeline_metadata (
    data_subject, source_system, frequency, source_path, landing_container,
    bronze_table, silver_table, gold_table,
    file_format, delimiter, has_header,
    null_check_columns, partition_column, apply_concat,
    group_columns, metric_columns, num_partitions
) VALUES (
    'o9_forecast_weekly', 'o9', 'weekly', 'o9/weekly/', 'landing',
    'bronze.o9_forecast_raw', 'silver.o9_forecast_ref', 'gold.o9_forecast_dmnsn',
    'CSV', '|', 1,
    'product_id,location_id,forecast_date', 'forecast_date', 1,
    'fl_nm', 'forecast_qty,revenue_amount', 8
);
GO

-- Monthly forecast: strategic planning, next 6-18 months
INSERT INTO audit.pipeline_metadata (
    data_subject, source_system, frequency, source_path, landing_container,
    bronze_table, silver_table, gold_table,
    file_format, delimiter, has_header,
    null_check_columns, partition_column, apply_concat,
    group_columns, metric_columns, num_partitions
) VALUES (
    'o9_forecast_monthly', 'o9', 'monthly', 'o9/monthly/', 'landing',
    'bronze.o9_forecast_raw', 'silver.o9_forecast_ref', 'gold.o9_forecast_dmnsn',
    'CSV', '|', 1,
    'product_id,location_id,forecast_date', 'forecast_date', 1,
    'fl_nm', 'forecast_qty,revenue_amount', 4
);
GO

-- Quarterly forecast: long-range/budget alignment
INSERT INTO audit.pipeline_metadata (
    data_subject, source_system, frequency, source_path, landing_container,
    bronze_table, silver_table, gold_table,
    file_format, delimiter, has_header,
    null_check_columns, partition_column, apply_concat,
    group_columns, metric_columns, num_partitions
) VALUES (
    'o9_forecast_quarterly', 'o9', 'quarterly', 'o9/quarterly/', 'landing',
    'bronze.o9_forecast_raw', 'silver.o9_forecast_ref', 'gold.o9_forecast_dmnsn',
    'CSV', '|', 1,
    'product_id,location_id,forecast_date', 'forecast_date', 1,
    'fl_nm', 'forecast_qty,revenue_amount', 4
);
GO

-- ============================================================
-- Stored Procedure: Insert Audit Entry
-- ============================================================
CREATE OR ALTER PROCEDURE audit.usp_insert_audit
    @batch_id           NVARCHAR(100),
    @application_id     NVARCHAR(200),
    @object_name        NVARCHAR(200),
    @data_layer         NVARCHAR(50),
    @job_status         NVARCHAR(20),
    @source_row_count   BIGINT = 0,
    @target_row_count   BIGINT = 0,
    @error_record_count BIGINT = 0,
    @source_system      NVARCHAR(100) = NULL,
    @file_name          NVARCHAR(500) = NULL,
    @load_job_number    NVARCHAR(100) = NULL,
    @created_by         NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_ts DATETIME2 = GETUTCDATE();

    INSERT INTO audit.pipeline_audit (
        batch_id, application_id, object_name, data_layer,
        job_start_ts, job_end_ts, job_status,
        source_row_count, target_row_count, error_record_count,
        job_duration_sec, source_system, file_name, load_job_number, created_by
    ) VALUES (
        @batch_id, @application_id, @object_name, @data_layer,
        @start_ts, GETUTCDATE(), @job_status,
        @source_row_count, @target_row_count, @error_record_count,
        DATEDIFF(SECOND, @start_ts, GETUTCDATE()),
        @source_system, @file_name, @load_job_number, @created_by
    );
END
GO

-- ============================================================
-- Stored Procedure: Update Audit Status
-- ============================================================
CREATE OR ALTER PROCEDURE audit.usp_update_audit_status
    @batch_id           NVARCHAR(100),
    @object_name        NVARCHAR(200),
    @data_layer         NVARCHAR(50),
    @job_status         NVARCHAR(20),
    @source_row_count   BIGINT = NULL,
    @target_row_count   BIGINT = NULL,
    @error_record_count BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE audit.pipeline_audit
    SET job_status = @job_status,
        job_end_ts = GETUTCDATE(),
        job_duration_sec = DATEDIFF(SECOND, job_start_ts, GETUTCDATE()),
        source_row_count = ISNULL(@source_row_count, source_row_count),
        target_row_count = ISNULL(@target_row_count, target_row_count),
        error_record_count = ISNULL(@error_record_count, error_record_count),
        updated_ts = GETUTCDATE()
    WHERE batch_id = @batch_id
      AND object_name = @object_name
      AND data_layer = @data_layer;
END
GO

-- ============================================================
-- View: Latest Pipeline Runs
-- ============================================================
CREATE OR ALTER VIEW audit.vw_latest_pipeline_runs AS
SELECT
    batch_id,
    object_name,
    data_layer,
    job_status,
    source_row_count,
    target_row_count,
    error_record_count,
    job_duration_sec,
    job_start_ts,
    job_end_ts,
    file_name,
    source_system
FROM audit.pipeline_audit
WHERE job_start_ts >= DATEADD(DAY, -7, GETUTCDATE())
GO
