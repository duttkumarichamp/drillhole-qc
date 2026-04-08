-- =============================================================================
-- Drillhole Data Quality Checker — T-SQL Schema & QC Stored Procedure
-- =============================================================================
-- Mirrors the same validation rules as validate.py, implemented server-side
-- using Microsoft SQL Server (T-SQL).
--
-- Objects created:
--   dbo.Collars            — drillhole collar master table
--   dbo.Intervals          — downhole interval table (lithology / assay)
--   dbo.QC_Issues          — audit log of all data quality findings
--   dbo.usp_RunDrillholeQC — stored procedure: runs all rules and populates QC_Issues
--   vw_QC_Summary          — view: aggregated pass/fail summary per hole
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Tables
-- ---------------------------------------------------------------------------

CREATE TABLE dbo.Collars (
    CollarID    INT             IDENTITY(1,1) PRIMARY KEY,
    HoleID      NVARCHAR(50)    NOT NULL,
    Easting     FLOAT           NULL,
    Northing    FLOAT           NULL,
    RL          FLOAT           NULL,
    MaxDepth    FLOAT           NULL,
    DrillDate   DATE            NULL,
    DrillType   NVARCHAR(10)    NULL,
    LoadedAt    DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_Collars_HoleID UNIQUE (HoleID)
);

CREATE TABLE dbo.Intervals (
    IntervalID  INT             IDENTITY(1,1) PRIMARY KEY,
    HoleID      NVARCHAR(50)    NOT NULL,
    FromDepth   FLOAT           NULL,
    ToDepth     FLOAT           NULL,
    Lithology   NVARCHAR(50)    NULL,
    Fe          FLOAT           NULL,
    Al2O3       FLOAT           NULL,
    SiO2        FLOAT           NULL,
    LoadedAt    DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.QC_Issues (
    IssueID     INT             IDENTITY(1,1) PRIMARY KEY,
    RunAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    Severity    NVARCHAR(10)    NOT NULL,   -- 'ERROR' | 'WARNING'
    SourceTable NVARCHAR(50)    NOT NULL,
    HoleID      NVARCHAR(50)    NULL,
    RuleCode    NVARCHAR(20)    NOT NULL,
    RuleName    NVARCHAR(100)   NOT NULL,
    Detail      NVARCHAR(500)   NULL
);
GO


-- ---------------------------------------------------------------------------
-- Stored Procedure: usp_RunDrillholeQC
-- ---------------------------------------------------------------------------
-- Clears the QC_Issues log for the current run scope and re-evaluates
-- every rule against the current contents of Collars and Intervals.
-- ---------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE dbo.usp_RunDrillholeQC
AS
BEGIN
    SET NOCOUNT ON;

    -- Clear previous run
    TRUNCATE TABLE dbo.QC_Issues;

    -- -------------------------------------------------------------------------
    -- COLLAR RULES
    -- -------------------------------------------------------------------------

    -- R-C01  Required fields present
    INSERT INTO dbo.QC_Issues (Severity, SourceTable, HoleID, RuleCode, RuleName, Detail)
    SELECT
        'ERROR', 'Collars', HoleID,
        'R-C01', 'Missing required field',
        CONCAT(
            CASE WHEN HoleID    IS NULL OR HoleID    = '' THEN 'HoleID, '    ELSE '' END,
            CASE WHEN Easting   IS NULL                   THEN 'Easting, '   ELSE '' END,
            CASE WHEN Northing  IS NULL                   THEN 'Northing, '  ELSE '' END,
            CASE WHEN MaxDepth  IS NULL                   THEN 'MaxDepth, '  ELSE '' END,
            CASE WHEN DrillDate IS NULL                   THEN 'DrillDate, ' ELSE '' END,
            CASE WHEN DrillType IS NULL OR DrillType = '' THEN 'DrillType'   ELSE '' END
        )
    FROM dbo.Collars
    WHERE
        HoleID    IS NULL OR HoleID    = ''
        OR Easting   IS NULL
        OR Northing  IS NULL
        OR MaxDepth  IS NULL
        OR DrillDate IS NULL
        OR DrillType IS NULL OR DrillType = '';

    -- R-C02  Duplicate HoleID
    INSERT INTO dbo.QC_Issues (Severity, SourceTable, HoleID, RuleCode, RuleName, Detail)
    SELECT
        'ERROR', 'Collars', HoleID,
        'R-C02', 'Duplicate HoleID',
        CONCAT('HoleID appears ', COUNT(*), ' times in Collars')
    FROM dbo.Collars
    GROUP BY HoleID
    HAVING COUNT(*) > 1;

    -- R-C03  Coordinate bounds (MGA94 WA)
    INSERT INTO dbo.QC_Issues (Severity, SourceTable, HoleID, RuleCode, RuleName, Detail)
    SELECT
        'ERROR', 'Collars', HoleID,
        'R-C03', 'Coordinate out of range',
        CONCAT(
            CASE WHEN Easting  NOT BETWEEN 300000   AND 900000   THEN CONCAT('Easting=', Easting, ' ')   ELSE '' END,
            CASE WHEN Northing NOT BETWEEN 6500000  AND 8500000  THEN CONCAT('Northing=', Northing)       ELSE '' END
        )
    FROM dbo.Collars
    WHERE
        (Easting  IS NOT NULL AND Easting  NOT BETWEEN 300000  AND 900000)
        OR (Northing IS NOT NULL AND Northing NOT BETWEEN 6500000 AND 8500000);

    -- R-C04  MaxDepth positive and within realistic range
    INSERT INTO dbo.QC_Issues (Severity, SourceTable, HoleID, RuleCode, RuleName, Detail)
    SELECT
        CASE WHEN MaxDepth <= 0 THEN 'ERROR' ELSE 'WARNING' END,
        'Collars', HoleID,
        'R-C04', 'MaxDepth invalid or suspect',
        CONCAT('MaxDepth = ', MaxDepth)
    FROM dbo.Collars
    WHERE MaxDepth IS NOT NULL
      AND (MaxDepth <= 0 OR MaxDepth < 1.0 OR MaxDepth > 3000.0);

    -- R-C05  DrillDate not in the future
    INSERT INTO dbo.QC_Issues (Severity, SourceTable, HoleID, RuleCode, RuleName, Detail)
    SELECT
        'WARNING', 'Collars', HoleID,
        'R-C05', 'DrillDate in future',
        CONCAT('DrillDate = ', CONVERT(NVARCHAR, DrillDate, 23))
    FROM dbo.Collars
    WHERE DrillDate > CAST(GETDATE() AS DATE);

    -- R-C06  DrillType in allowed list
    INSERT INTO dbo.QC_Issues (Severity, SourceTable, HoleID, RuleCode, RuleName, Detail)
    SELECT
        'ERROR', 'Collars', HoleID,
        'R-C06', 'Unrecognised DrillType',
        CONCAT('DrillType = ''', DrillType, ''' — expected one of: RC, DD, AC, RAB')
    FROM dbo.Collars
    WHERE UPPER(TRIM(DrillType)) NOT IN ('RC', 'DD', 'AC', 'RAB');

    -- -------------------------------------------------------------------------
    -- INTERVAL RULES
    -- -------------------------------------------------------------------------

    -- R-I01  HoleID in Intervals must exist in Collars
    INSERT INTO dbo.QC_Issues (Severity, SourceTable, HoleID, RuleCode, RuleName, Detail)
    SELECT DISTINCT
        'ERROR', 'Intervals', i.HoleID,
        'R-I01', 'HoleID not in Collars',
        CONCAT('''', i.HoleID, ''' has no matching collar record')
    FROM dbo.Intervals i
    LEFT JOIN dbo.Collars c ON c.HoleID = i.HoleID
    WHERE c.HoleID IS NULL;

    -- R-I03  From < To
    INSERT INTO dbo.QC_Issues (Severity, SourceTable, HoleID, RuleCode, RuleName, Detail)
    SELECT
        'ERROR', 'Intervals', HoleID,
        'R-I03', 'From >= To',
        CONCAT('IntervalID=', IntervalID, '  From=', FromDepth, '  To=', ToDepth)
    FROM dbo.Intervals
    WHERE FromDepth IS NOT NULL
      AND ToDepth   IS NOT NULL
      AND FromDepth >= ToDepth;

    -- R-I04  Assay values within physical bounds
    INSERT INTO dbo.QC_Issues (Severity, SourceTable, HoleID, RuleCode, RuleName, Detail)
    SELECT 'ERROR', 'Intervals', HoleID, 'R-I04', 'Assay out of range', Detail
    FROM (
        SELECT HoleID, CONCAT('Fe=',    Fe,    ' outside [0,100]') AS Detail FROM dbo.Intervals WHERE Fe    IS NOT NULL AND (Fe    < 0 OR Fe    > 100)
        UNION ALL
        SELECT HoleID, CONCAT('Al2O3=', Al2O3, ' outside [0,100]') AS Detail FROM dbo.Intervals WHERE Al2O3 IS NOT NULL AND (Al2O3 < 0 OR Al2O3 > 100)
        UNION ALL
        SELECT HoleID, CONCAT('SiO2=',  SiO2,  ' outside [0,100]') AS Detail FROM dbo.Intervals WHERE SiO2  IS NOT NULL AND (SiO2  < 0 OR SiO2  > 100)
    ) assay_issues;

    -- R-I05  Overlapping intervals within a hole
    INSERT INTO dbo.QC_Issues (Severity, SourceTable, HoleID, RuleCode, RuleName, Detail)
    SELECT DISTINCT
        'ERROR', 'Intervals', a.HoleID,
        'R-I05', 'Overlapping intervals',
        CONCAT(
            'Interval [', a.FromDepth, ',', a.ToDepth, '] (ID=', a.IntervalID, ')',
            ' overlaps [', b.FromDepth, ',', b.ToDepth, '] (ID=', b.IntervalID, ')'
        )
    FROM dbo.Intervals a
    INNER JOIN dbo.Intervals b
        ON  a.HoleID     = b.HoleID
        AND a.IntervalID < b.IntervalID
        AND a.ToDepth    > b.FromDepth
        AND a.FromDepth  < b.ToDepth;

    -- -------------------------------------------------------------------------
    -- Summary
    -- -------------------------------------------------------------------------
    SELECT
        Severity,
        COUNT(*) AS IssueCount
    FROM dbo.QC_Issues
    GROUP BY Severity
    ORDER BY Severity;

END;
GO


-- ---------------------------------------------------------------------------
-- View: vw_QC_Summary — pass/fail per hole
-- ---------------------------------------------------------------------------

CREATE OR ALTER VIEW dbo.vw_QC_Summary AS
SELECT
    c.HoleID,
    c.DrillType,
    c.MaxDepth,
    c.DrillDate,
    COUNT(q.IssueID)                                        AS TotalIssues,
    SUM(CASE WHEN q.Severity = 'ERROR'   THEN 1 ELSE 0 END) AS Errors,
    SUM(CASE WHEN q.Severity = 'WARNING' THEN 1 ELSE 0 END) AS Warnings,
    CASE
        WHEN SUM(CASE WHEN q.Severity = 'ERROR' THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS QCStatus
FROM dbo.Collars c
LEFT JOIN dbo.QC_Issues q ON q.HoleID = c.HoleID
GROUP BY c.HoleID, c.DrillType, c.MaxDepth, c.DrillDate;
GO


-- ---------------------------------------------------------------------------
-- Example usage
-- ---------------------------------------------------------------------------

-- EXEC dbo.usp_RunDrillholeQC;
-- SELECT * FROM dbo.QC_Issues   ORDER BY Severity, SourceTable, HoleID;
-- SELECT * FROM dbo.vw_QC_Summary ORDER BY QCStatus, HoleID;
