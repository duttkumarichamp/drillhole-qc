<<<<<<< HEAD
# Drillhole Data Quality Checker

A lightweight, rule-based QC tool for validating geoscience drillhole data (collars and downhole intervals) before loading into a centralised drilling database.

Implements the same validation rules in both **Python** (for file-based / pre-load validation) and **T-SQL** (for server-side validation within a SQL Server database), so checks can be run at multiple points in the data lifecycle.

---

## Why this exists

In exploration and mining data workflows, data quality issues — duplicate hole IDs, overlapping intervals, out-of-range assay values, missing coordinates — are common and costly when they propagate downstream into resource models and decisions. This tool catches them early, at the point of ingest.

---

## Project structure

```
drillhole-qc/
├── validate.py               # Python QC checker (CSV → console + report)
├── schema.sql                # T-SQL schema, stored procedure, and summary view
├── sample_data/
│   ├── collars.csv           # Sample collar data (with intentional QC issues)
│   └── intervals.csv         # Sample interval data (lithology + assay)
└── qc_report.csv             # Generated output (after running validate.py)
```

---

## QC rules implemented

| Code  | Table     | Rule                                        | Severity |
|-------|-----------|---------------------------------------------|----------|
| R-C01 | Collars   | Required fields present                     | ERROR    |
| R-C02 | Collars   | No duplicate HoleIDs                        | ERROR    |
| R-C03 | Collars   | Easting/Northing within MGA94 WA bounds     | ERROR    |
| R-C04 | Collars   | MaxDepth positive and within realistic range| ERROR / WARNING |
| R-C05 | Collars   | DrillDate is valid and not in the future    | ERROR / WARNING |
| R-C06 | Collars   | DrillType in allowed list (RC, DD, AC, RAB) | ERROR    |
| R-I01 | Intervals | HoleID exists in Collars                    | ERROR    |
| R-I02 | Intervals | From and To are numeric                     | ERROR    |
| R-I03 | Intervals | From < To (no zero-length or inverted intervals) | ERROR |
| R-I04 | Intervals | Assay values within physical bounds (0–100%)| ERROR    |
| R-I05 | Intervals | No overlapping intervals within a hole      | ERROR    |

---

## Python usage

```bash
# Run against sample data
python validate.py

# Run against your own files
python validate.py --collars path/to/collars.csv --intervals path/to/intervals.csv

# Specify a custom output report path
python validate.py --output my_qc_report.csv
```

**No external dependencies** — uses only the Python standard library.

### Example output

```
=================================================================
  DRILLHOLE DATA QUALITY REPORT
=================================================================
  Total issues : 12  (10 errors, 2 warnings)
=================================================================

  [COLLARS]
  ✗ ERROR    Row 4     WAIO_004     R-C01: Missing required field
             → Column 'Easting' is empty or absent
  ✗ ERROR    Row 5     WAIO_005     R-C04: MaxDepth non-positive
             → MaxDepth = -50.0
  ✗ ERROR    Row 6     WAIO_002     R-C02: Duplicate HoleID
             → First seen at row 2
  ...
```

---

## T-SQL usage

1. Run `schema.sql` against your SQL Server database to create all objects.
2. Load your data into `dbo.Collars` and `dbo.Intervals`.
3. Execute the stored procedure:

```sql
EXEC dbo.usp_RunDrillholeQC;

-- Review all issues
SELECT * FROM dbo.QC_Issues ORDER BY Severity, SourceTable, HoleID;

-- Per-hole pass/fail summary
SELECT * FROM dbo.vw_QC_Summary ORDER BY QCStatus, HoleID;
```

---

## Extending the ruleset

Rules are modular in both implementations:

- **Python**: add a new validation block inside `validate_collars()` or `validate_intervals()`, calling `report.add(...)` with the appropriate severity and rule code.
- **T-SQL**: add a new `INSERT INTO dbo.QC_Issues` block inside `usp_RunDrillholeQC`.

Both follow the same rule code convention (`R-C##` for collar rules, `R-I##` for interval rules) to keep the two implementations aligned.

---

## Tech stack

- Python 3.9 (standard library only)
- Microsoft SQL Server / T-SQL (compatible with SQL Server 2016+)
=======
# drillhole-qc
Python and T-SQL drillhole data quality checker for validating collar and interval data before database load
>>>>>>> 06ed243613175a6d6cbf60afaac846a67eaba8f1
