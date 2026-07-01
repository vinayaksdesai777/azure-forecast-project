-- ============================================================
-- Azure SQL Database: Source Extraction Metadata
-- Drives pl_extract_to_landing (source systems -> landing CSV).
-- One row per source object to pull into the landing container.
-- Run AFTER 01_audit_tables.sql.
-- ============================================================

-- ============================================================
-- Source Extract Metadata Table (one row per source object)
-- ============================================================
IF OBJECT_ID('audit.source_extract_metadata', 'U') IS NULL
BEGIN
    CREATE TABLE audit.source_extract_metadata (
        extract_id          INT IDENTITY(1,1) PRIMARY KEY,
        source_system       NVARCHAR(100) NOT NULL,      -- SAP_HANA, SQL_SERVER, SALESFORCE
        connector           NVARCHAR(50)  NOT NULL,      -- SapHana, SqlServer, Salesforce (Switch value in pipeline)
        source_schema       NVARCHAR(200),               -- e.g. SAPABAP1, dbo (NULL/unused for Salesforce)
        source_object       NVARCHAR(200) NOT NULL,      -- table/view/SOQL object
        landing_path        NVARCHAR(500) NOT NULL,      -- folder under the landing container (matches pipeline_metadata.source_path)
        data_subject        NVARCHAR(100) NOT NULL,      -- links back to pipeline_metadata.data_subject
        load_type           NVARCHAR(20)  NOT NULL DEFAULT 'full',   -- full | incremental
        watermark_column    NVARCHAR(200),               -- column used for incremental high-watermark
        last_watermark      NVARCHAR(100),               -- last successfully extracted high-watermark value
        is_active           BIT DEFAULT 1,
        created_ts          DATETIME2 DEFAULT GETUTCDATE(),
        updated_ts          DATETIME2 DEFAULT GETUTCDATE()
    );

    CREATE INDEX IX_source_extract_active ON audit.source_extract_metadata(is_active);
    CREATE UNIQUE INDEX UX_source_extract_object ON audit.source_extract_metadata(source_system, source_object);
END
GO

-- ============================================================
-- Stored Procedure: Update Extract Watermark
-- Called by pl_extract_to_landing after each successful copy.
-- ============================================================
CREATE OR ALTER PROCEDURE audit.usp_update_extract_watermark
    @source_system  NVARCHAR(100),
    @source_object  NVARCHAR(200),
    @new_watermark  NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE audit.source_extract_metadata
    SET last_watermark = @new_watermark,
        updated_ts     = GETUTCDATE()
    WHERE source_system = @source_system
      AND source_object = @source_object
      AND load_type     = 'incremental';
END
GO

-- ============================================================
-- Seed: one extract row per data subject / source object.
-- landing_path MUST match the corresponding pipeline_metadata.source_path
-- so the master pipeline's GetMetadata finds the dropped CSV.
-- ============================================================
INSERT INTO audit.source_extract_metadata
    (source_system, connector, source_schema, source_object, landing_path, data_subject, load_type, watermark_column, last_watermark, is_active)
VALUES
    ('SAP_HANA',   'SapHana',    'O9_SOURCE', 'FORECAST_VIEW', 'o9/daily/',     'o9_forecast_daily',     'incremental', 'CHANGED_ON',  '1900-01-01 00:00:00', 1),
    ('SQL_SERVER', 'SqlServer',  'dbo',      'Forecast',      'o9/weekly/',    'o9_forecast_weekly',    'incremental', 'modified_dt', '1900-01-01 00:00:00', 1),
    ('SALESFORCE', 'Salesforce', NULL,       'Forecast__c',   'o9/monthly/',   'o9_forecast_monthly',   'full',        NULL,          NULL,                  1),
    ('SALESFORCE', 'Salesforce', NULL,       'Forecast__c',   'o9/quarterly/', 'o9_forecast_quarterly', 'full',        NULL,          NULL,                  1);
GO

-- ============================================================
-- Align pipeline_metadata.source_system with the real source of record
-- (previously all 'o9'). Records provenance per layer in the audit trail.
-- ============================================================
UPDATE audit.pipeline_metadata SET source_system = 'SAP_HANA'   WHERE data_subject = 'o9_forecast_daily';
UPDATE audit.pipeline_metadata SET source_system = 'SQL_SERVER' WHERE data_subject = 'o9_forecast_weekly';
UPDATE audit.pipeline_metadata SET source_system = 'SALESFORCE' WHERE data_subject IN ('o9_forecast_monthly', 'o9_forecast_quarterly');
GO
