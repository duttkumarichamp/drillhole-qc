"""
Drillhole Data Quality Checker
================================
Validates geoscience drillhole CSVs (collars + intervals) against a set of
configurable QC rules. Outputs a structured report to console and a CSV log.

Usage:
    python validate.py
    python validate.py --collars path/to/collars.csv --intervals path/to/intervals.csv
"""

import argparse
import csv
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

VALID_DRILL_TYPES = {"RC", "DD", "AC", "RAB"}
DATE_FORMAT = "%Y-%m-%d"

COLLAR_EASTING_RANGE  = (300_000, 900_000)   # MGA94 Zone 50/51 bounds (WA)
COLLAR_NORTHING_RANGE = (6_500_000, 8_500_000)

MIN_MAX_DEPTH = 1.0      # metres
MAX_MAX_DEPTH = 3_000.0  # metres

ASSAY_BOUNDS = {          # column : (min_valid, max_valid)
    "Fe":    (0.0, 100.0),
    "Al2O3": (0.0, 100.0),
    "SiO2":  (0.0, 100.0),
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class QCIssue:
    severity: str          # "ERROR" | "WARNING"
    table: str             # "collars" | "intervals"
    row: int               # 1-based row number in source file
    hole_id: str
    rule: str
    detail: str


@dataclass
class QCReport:
    issues: list[QCIssue] = field(default_factory=list)

    def add(self, severity, table, row, hole_id, rule, detail):
        self.issues.append(QCIssue(severity, table, row, hole_id, rule, detail))

    @property
    def errors(self):
        return [i for i in self.issues if i.severity == "ERROR"]

    @property
    def warnings(self):
        return [i for i in self.issues if i.severity == "WARNING"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _float(value: str, default=None) -> Optional[float]:
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _parse_date(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value.strip(), DATE_FORMAT)
    except (ValueError, TypeError):
        return None


def _read_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# Collar validation rules
# ---------------------------------------------------------------------------

def validate_collars(rows: list[dict], report: QCReport) -> set[str]:
    """Returns the set of valid HoleIDs after all collar checks."""
    seen_ids: dict[str, int] = {}
    valid_ids: set[str] = set()

    required_cols = {"HoleID", "Easting", "Northing", "RL", "MaxDepth", "DrillDate", "DrillType"}

    for row_num, row in enumerate(rows, start=2):  # row 1 = header

        hole_id = row.get("HoleID", "").strip()
        row_tag = hole_id or f"<row {row_num}>"

        # R-C01  Required fields present
        for col in required_cols:
            if col not in row or row[col].strip() == "":
                report.add("ERROR", "collars", row_num, row_tag,
                           "R-C01: Missing required field",
                           f"Column '{col}' is empty or absent")

        if not hole_id:
            continue  # can't do further checks without an ID

        # R-C02  Duplicate HoleID
        if hole_id in seen_ids:
            report.add("ERROR", "collars", row_num, hole_id,
                       "R-C02: Duplicate HoleID",
                       f"First seen at row {seen_ids[hole_id]}")
            continue
        seen_ids[hole_id] = row_num

        # R-C03  Coordinate sanity (basic MGA bounds for WA)
        easting  = _float(row.get("Easting", ""))
        northing = _float(row.get("Northing", ""))
        if easting is not None and not (COLLAR_EASTING_RANGE[0] <= easting <= COLLAR_EASTING_RANGE[1]):
            report.add("ERROR", "collars", row_num, hole_id,
                       "R-C03: Easting out of range",
                       f"Value {easting} outside expected range {COLLAR_EASTING_RANGE}")
        if northing is not None and not (COLLAR_NORTHING_RANGE[0] <= northing <= COLLAR_NORTHING_RANGE[1]):
            report.add("ERROR", "collars", row_num, hole_id,
                       "R-C03: Northing out of range",
                       f"Value {northing} outside expected range {COLLAR_NORTHING_RANGE}")

        # R-C04  MaxDepth positive and within realistic bounds
        max_depth = _float(row.get("MaxDepth", ""))
        if max_depth is None:
            report.add("ERROR", "collars", row_num, hole_id,
                       "R-C04: MaxDepth not numeric", "Cannot parse MaxDepth as a number")
        elif max_depth <= 0:
            report.add("ERROR", "collars", row_num, hole_id,
                       "R-C04: MaxDepth non-positive", f"MaxDepth = {max_depth}")
        elif max_depth < MIN_MAX_DEPTH:
            report.add("WARNING", "collars", row_num, hole_id,
                       "R-C04: MaxDepth suspiciously shallow", f"MaxDepth = {max_depth} m")
        elif max_depth > MAX_MAX_DEPTH:
            report.add("WARNING", "collars", row_num, hole_id,
                       "R-C04: MaxDepth suspiciously deep", f"MaxDepth = {max_depth} m")

        # R-C05  DrillDate parseable and not in future
        drill_date = _parse_date(row.get("DrillDate", ""))
        if drill_date is None:
            report.add("ERROR", "collars", row_num, hole_id,
                       "R-C05: Invalid DrillDate",
                       f"'{row.get('DrillDate', '')}' is not a valid {DATE_FORMAT} date")
        elif drill_date > datetime.now():
            report.add("WARNING", "collars", row_num, hole_id,
                       "R-C05: DrillDate in future", f"Date = {drill_date.date()}")

        # R-C06  DrillType in allowed list
        drill_type = row.get("DrillType", "").strip().upper()
        if drill_type not in VALID_DRILL_TYPES:
            report.add("ERROR", "collars", row_num, hole_id,
                       "R-C06: Unrecognised DrillType",
                       f"'{drill_type}' not in {sorted(VALID_DRILL_TYPES)}")

        valid_ids.add(hole_id)

    return valid_ids


# ---------------------------------------------------------------------------
# Interval validation rules
# ---------------------------------------------------------------------------

def validate_intervals(rows: list[dict], valid_collar_ids: set[str], report: QCReport):

    # Group intervals by hole for overlap / gap checks
    holes: dict[str, list[tuple[int, float, float]]] = {}

    for row_num, row in enumerate(rows, start=2):
        hole_id = row.get("HoleID", "").strip()
        row_tag = hole_id or f"<row {row_num}>"

        # R-I01  HoleID exists in collars
        if hole_id and hole_id not in valid_collar_ids:
            report.add("ERROR", "intervals", row_num, hole_id,
                       "R-I01: HoleID not in collars",
                       f"'{hole_id}' has no matching collar record")

        from_val = _float(row.get("From", ""))
        to_val   = _float(row.get("To", ""))

        # R-I02  From / To are numeric
        if from_val is None:
            report.add("ERROR", "intervals", row_num, row_tag,
                       "R-I02: From not numeric", f"Value: '{row.get('From', '')}'")
        if to_val is None:
            report.add("ERROR", "intervals", row_num, row_tag,
                       "R-I02: To not numeric", f"Value: '{row.get('To', '')}'")

        if from_val is not None and to_val is not None:
            # R-I03  From < To (zero-length and inverted intervals)
            if from_val >= to_val:
                report.add("ERROR", "intervals", row_num, row_tag,
                           "R-I03: From >= To",
                           f"From={from_val}, To={to_val} — interval has no length")

            # Collect for overlap check
            if hole_id:
                holes.setdefault(hole_id, []).append((row_num, from_val, to_val))

        # R-I04  Assay values within physical bounds
        for col, (lo, hi) in ASSAY_BOUNDS.items():
            raw = row.get(col, "").strip()
            if raw == "":
                continue
            val = _float(raw)
            if val is None:
                report.add("ERROR", "intervals", row_num, row_tag,
                           f"R-I04: {col} not numeric", f"Value: '{raw}'")
            elif not (lo <= val <= hi):
                report.add("ERROR", "intervals", row_num, row_tag,
                           f"R-I04: {col} out of range",
                           f"{col}={val} outside [{lo}, {hi}]")

    # R-I05  Overlapping intervals within a hole
    for hole_id, intervals in holes.items():
        sorted_ivs = sorted(intervals, key=lambda x: x[1])  # sort by From
        for i in range(len(sorted_ivs) - 1):
            r1, f1, t1 = sorted_ivs[i]
            r2, f2, t2 = sorted_ivs[i + 1]
            if t1 > f2:
                report.add("ERROR", "intervals", r2, hole_id,
                           "R-I05: Overlapping intervals",
                           f"Interval [{f1}, {t1}] (row {r1}) overlaps [{f2}, {t2}] (row {r2})")


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(report: QCReport):
    total = len(report.issues)
    errors   = len(report.errors)
    warnings = len(report.warnings)

    print("\n" + "=" * 65)
    print("  DRILLHOLE DATA QUALITY REPORT")
    print("=" * 65)
    print(f"  Total issues : {total}  ({errors} errors, {warnings} warnings)")
    print("=" * 65)

    if not report.issues:
        print("  ✓  No issues found. Data passes all QC rules.")
    else:
        # Group by table
        for table in ("collars", "intervals"):
            section = [i for i in report.issues if i.table == table]
            if not section:
                continue
            print(f"\n  [{table.upper()}]")
            for issue in section:
                tag = "✗ ERROR  " if issue.severity == "ERROR" else "⚠ WARNING"
                print(f"  {tag}  Row {issue.row:<4}  {issue.hole_id:<12}  {issue.rule}")
                print(f"             → {issue.detail}")

    print("\n" + "=" * 65 + "\n")


def write_csv_report(report: QCReport, output_path: str):
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["Severity", "Table", "Row", "HoleID", "Rule", "Detail"])
        for issue in report.issues:
            writer.writerow([
                issue.severity, issue.table, issue.row,
                issue.hole_id, issue.rule, issue.detail
            ])
    print(f"  Report saved → {output_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Drillhole CSV Data Quality Checker")
    parser.add_argument("--collars",   default="sample_data/collars.csv")
    parser.add_argument("--intervals", default="sample_data/intervals.csv")
    parser.add_argument("--output",    default="qc_report.csv",
                        help="Output CSV report path")
    args = parser.parse_args()

    for path in (args.collars, args.intervals):
        if not os.path.exists(path):
            print(f"ERROR: File not found: {path}", file=sys.stderr)
            sys.exit(1)

    print(f"\nLoading collars   : {args.collars}")
    print(f"Loading intervals : {args.intervals}")

    collars_rows   = _read_csv(args.collars)
    intervals_rows = _read_csv(args.intervals)

    report = QCReport()

    valid_ids = validate_collars(collars_rows, report)
    validate_intervals(intervals_rows, valid_ids, report)

    print_report(report)
    write_csv_report(report, args.output)

    # Exit code: 1 if any errors, 0 if only warnings or clean
    sys.exit(1 if report.errors else 0)


if __name__ == "__main__":
    main()
