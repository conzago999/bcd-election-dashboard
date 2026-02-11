"""
BCD Data Quality Module
Assesses and tracks confidence scores for election data.
Each election gets a 0.0-1.0 confidence score based on source quality,
parse integrity, and cross-validation status.
"""

import sqlite3
import os
from datetime import datetime

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

from database import get_connection, DB_PATH, init_db


# ---------------------------------------------------------------------------
# Weights for each quality factor (must sum to 1.0)
# ---------------------------------------------------------------------------
WEIGHTS = {
    "source_type": 0.25,
    "race_names_clean": 0.20,
    "turnout_consistent": 0.20,
    "precinct_count_match": 0.15,
    "cross_validated": 0.20,
}

# Source type scores (contributes via WEIGHTS["source_type"])
SOURCE_TYPE_SCORES = {
    "digital_pdf": 1.0,
    "excel": 0.9,
    "scanned_pdf": 0.3,
    "manual_entry": 0.5,
}

# Known election-level overrides based on domain knowledge.
# Keys are election_date strings; values override specific fields.
KNOWN_OVERRIDES = {
    # 2010 primaries/generals: re-imported from digital PDFs but not cross-validated
    "2010-05-04": {
        "cross_validated": 0,
        "notes": "Re-imported from digital PDF; not fully cross-validated.",
    },
    "2010-11-02": {
        "cross_validated": 0,
        "notes": "Re-imported from digital PDF; not fully cross-validated.",
    },
    # 2011 general: scanned PDF, 43 corrupted race names, OCR not applied
    "2011-11-08": {
        "source_type": "scanned_pdf",
        "cross_validated": 0,
        "notes": "Scanned PDF; 43 corrupted race names remain; OCR not applied.",
    },
    # 2012: re-imported from digital PDFs but not cross-validated
    "2012-05-08": {
        "cross_validated": 0,
        "notes": "Re-imported from digital PDF; not fully cross-validated.",
    },
    "2012-11-06": {
        "cross_validated": 0,
        "notes": "Re-imported from digital PDF; not fully cross-validated.",
    },
    # 2014: re-imported from digital PDFs but not cross-validated
    "2014-05-06": {
        "cross_validated": 0,
        "notes": "Re-imported from digital PDF; not fully cross-validated.",
    },
    "2014-11-04": {
        "cross_validated": 0,
        "notes": "Re-imported from digital PDF; not fully cross-validated.",
    },
    # 2015: re-imported, not fully cross-validated
    "2015-05-05": {
        "cross_validated": 0,
        "notes": "Re-imported from digital PDF; not fully cross-validated.",
    },
    "2015-11-03": {
        "cross_validated": 0,
        "notes": "Re-imported from digital PDF; not fully cross-validated.",
    },
    # 2017 special: only 1 race / 4 results -- tiny dataset, limited validation
    "2017-05-02": {
        "cross_validated": 0,
        "notes": "Very small election: 1 race, 4 results. Limited validation possible.",
    },
    # 2023-05-02 primary: sparse data
    "2023-05-02": {
        "notes": "Sparse data: 21 races, 37 results; no turnout records.",
    },
}


def _detect_source_type(source_file, import_file_type, election_date):
    """Determine the source type for an election.

    Priority:
      1. Known overrides (e.g. 2011 scanned PDF)
      2. import_log file_type = 'pdf_reimport' -> 'digital_pdf' (re-parsed)
      3. File extension heuristics
    """
    date_str = str(election_date)

    # Check overrides first
    if date_str in KNOWN_OVERRIDES and "source_type" in KNOWN_OVERRIDES[date_str]:
        return KNOWN_OVERRIDES[date_str]["source_type"]

    if import_file_type == "pdf_reimport":
        return "digital_pdf"

    if source_file:
        ext = os.path.splitext(source_file)[1].lower()
        if ext in (".xlsx", ".xls", ".csv"):
            return "excel"
        if ext == ".pdf":
            return "digital_pdf"

    return "manual_entry"


def _check_race_name_integrity(conn, election_id):
    """Return (is_clean, corrupted_count).

    Corrupted race names match the glob pattern with embedded numeric
    percentage fragments like '42 58.33%'.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM races WHERE election_id = ? "
        "AND race_name GLOB '*[0-9] [0-9]*[0-9]%*'",
        (election_id,),
    )
    corrupted = cursor.fetchone()[0]
    return (1 if corrupted == 0 else 0, corrupted)


def _check_turnout_consistency(conn, election_id):
    """Check whether sum of precinct-level ballots_cast in the turnout table
    roughly matches the total results count for the election.

    We compare total turnout ballots to total votes across all races/candidates.
    Since each voter casts votes in multiple races, we just check that
    turnout data exists and is non-zero.

    Returns (is_consistent, detail_string).
    """
    cursor = conn.cursor()

    # Total ballots from turnout table (precinct-level, non-null precinct_id)
    cursor.execute(
        "SELECT COALESCE(SUM(ballots_cast), 0) FROM turnout "
        "WHERE election_id = ? AND precinct_id IS NOT NULL",
        (election_id,),
    )
    turnout_total = cursor.fetchone()[0]

    # Total ballots from the election header
    cursor.execute(
        "SELECT total_ballots_cast FROM elections WHERE id = ?",
        (election_id,),
    )
    row = cursor.fetchone()
    header_total = row[0] if row and row[0] else 0

    if turnout_total == 0 and header_total == 0:
        return (0, "No turnout data available")

    if turnout_total > 0 and header_total > 0:
        ratio = turnout_total / header_total if header_total else 0
        # Allow some tolerance -- precinct sums can legitimately differ
        if 0.9 <= ratio <= 1.1:
            return (1, f"Turnout {turnout_total} vs header {header_total} (ratio {ratio:.2f})")
        else:
            return (0, f"Mismatch: turnout {turnout_total} vs header {header_total} (ratio {ratio:.2f})")

    if turnout_total > 0:
        return (1, f"Turnout data present ({turnout_total} ballots), no header total")

    return (0, f"No precinct turnout data; header says {header_total}")


def _check_precinct_count(conn, election_id):
    """Check whether the number of precincts with turnout data is reasonable.

    Boone County typically has ~53 precincts in recent elections.
    We check that we have turnout records for a meaningful number of precincts.

    Returns (matches, precinct_count).
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(DISTINCT precinct_id) FROM turnout "
        "WHERE election_id = ? AND precinct_id IS NOT NULL",
        (election_id,),
    )
    count = cursor.fetchone()[0]

    # Also count precincts that have results
    cursor.execute(
        "SELECT COUNT(DISTINCT r2.precinct_id) FROM results r2 "
        "JOIN races r ON r2.race_id = r.id "
        "WHERE r.election_id = ? AND r2.precinct_id IS NOT NULL",
        (election_id,),
    )
    result_precinct_count = cursor.fetchone()[0]

    effective_count = max(count, result_precinct_count)

    # Also count total results to judge if data is substantive
    cursor.execute(
        "SELECT COUNT(*) FROM results r2 "
        "JOIN races r ON r2.race_id = r.id "
        "WHERE r.election_id = ?",
        (election_id,),
    )
    total_results = cursor.fetchone()[0]

    # Consider it a match if:
    # - We have at least 15 precincts (covers smaller elections), OR
    # - We have a single precinct but many results (county-wide aggregate format)
    if effective_count >= 15:
        return (1, effective_count)
    elif effective_count >= 1 and total_results >= 100:
        # County-wide aggregate: single precinct holding all results is valid
        return (1, effective_count)
    elif effective_count > 0:
        return (0, effective_count)
    else:
        return (0, 0)


def _determine_cross_validated(election_date, source_type):
    """Determine whether this election has been cross-validated against its PDF.

    Heuristic:
      - 2016+ digital PDFs: considered validated (imported with mature pipeline)
      - 2010-2015 reimports: not fully cross-validated yet
      - 2011 scanned: not validated
    """
    date_str = str(election_date)
    year = int(date_str[:4])

    # Check overrides
    if date_str in KNOWN_OVERRIDES and "cross_validated" in KNOWN_OVERRIDES[date_str]:
        return KNOWN_OVERRIDES[date_str]["cross_validated"]

    if year >= 2016 and source_type == "digital_pdf":
        return 1

    return 0


def _compute_confidence_score(source_type, race_clean, turnout_ok, precinct_ok, cross_val):
    """Compute a weighted 0.0-1.0 confidence score from the individual checks."""
    source_score = SOURCE_TYPE_SCORES.get(source_type, 0.5)
    score = (
        WEIGHTS["source_type"] * source_score
        + WEIGHTS["race_names_clean"] * race_clean
        + WEIGHTS["turnout_consistent"] * turnout_ok
        + WEIGHTS["precinct_count_match"] * precinct_ok
        + WEIGHTS["cross_validated"] * cross_val
    )
    return round(score, 3)


def _score_to_label(score):
    """Convert a numeric score to a categorical label."""
    if score >= 0.85:
        return "high"
    elif score >= 0.45:
        return "medium"
    else:
        return "low"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def assess_election_confidence(election_id=None, election_date=None, db_path=None):
    """Assess data quality for a single election.

    Provide either election_id or election_date.
    Returns an assessment dict and inserts/updates the data_quality table.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Resolve election
    if election_id is not None:
        cursor.execute(
            "SELECT id, election_date, election_name, source_file "
            "FROM elections WHERE id = ?",
            (election_id,),
        )
    elif election_date is not None:
        cursor.execute(
            "SELECT id, election_date, election_name, source_file "
            "FROM elections WHERE election_date = ?",
            (election_date,),
        )
    else:
        conn.close()
        raise ValueError("Must provide either election_id or election_date")

    row = cursor.fetchone()
    if not row:
        conn.close()
        raise ValueError(f"No election found for id={election_id}, date={election_date}")

    eid = row[0]
    edate = row[1]
    ename = row[2]
    source_file = row[3]

    # Check import_log for file_type
    import_file_type = None
    if source_file:
        cursor.execute(
            "SELECT file_type FROM import_log WHERE filename = ? ORDER BY imported_at DESC LIMIT 1",
            (source_file,),
        )
        il_row = cursor.fetchone()
        if il_row:
            import_file_type = il_row[0]

    # --- Run checks ---
    source_type = _detect_source_type(source_file, import_file_type, edate)
    race_clean, corrupted_count = _check_race_name_integrity(conn, eid)
    turnout_ok, turnout_detail = _check_turnout_consistency(conn, eid)
    precinct_ok, precinct_count = _check_precinct_count(conn, eid)
    cross_val = _determine_cross_validated(edate, source_type)

    # Determine if PDF parsed cleanly
    pdf_parsed_ok = 1
    if source_type == "scanned_pdf":
        pdf_parsed_ok = 0
    if corrupted_count > 0:
        pdf_parsed_ok = 0

    # Compute score
    score = _compute_confidence_score(source_type, race_clean, turnout_ok, precinct_ok, cross_val)
    label = _score_to_label(score)

    # Build notes
    notes_parts = []
    date_str = str(edate)
    if date_str in KNOWN_OVERRIDES and "notes" in KNOWN_OVERRIDES[date_str]:
        notes_parts.append(KNOWN_OVERRIDES[date_str]["notes"])
    if corrupted_count > 0:
        notes_parts.append(f"{corrupted_count} corrupted race names detected.")
    notes_parts.append(f"Precincts with data: {precinct_count}.")
    notes_parts.append(turnout_detail + ".")
    notes = " ".join(notes_parts)

    # Insert or update
    cursor.execute("SELECT id FROM data_quality WHERE election_id = ?", (eid,))
    existing = cursor.fetchone()

    if existing:
        cursor.execute(
            """UPDATE data_quality SET
                overall_confidence = ?, confidence_score = ?, source_type = ?,
                pdf_parsed_ok = ?, cross_validated = ?, race_names_clean = ?,
                turnout_consistent = ?, precinct_count_match = ?,
                notes = ?, assessed_at = CURRENT_TIMESTAMP
            WHERE election_id = ?""",
            (label, score, source_type, pdf_parsed_ok, cross_val,
             race_clean, turnout_ok, precinct_ok, notes, eid),
        )
    else:
        cursor.execute(
            """INSERT INTO data_quality
                (election_id, overall_confidence, confidence_score, source_type,
                 pdf_parsed_ok, cross_validated, race_names_clean,
                 turnout_consistent, precinct_count_match, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (eid, label, score, source_type, pdf_parsed_ok, cross_val,
             race_clean, turnout_ok, precinct_ok, notes),
        )

    conn.commit()
    conn.close()

    assessment = {
        "election_id": eid,
        "election_date": edate,
        "election_name": ename,
        "overall_confidence": label,
        "confidence_score": score,
        "source_type": source_type,
        "pdf_parsed_ok": pdf_parsed_ok,
        "cross_validated": cross_val,
        "race_names_clean": race_clean,
        "turnout_consistent": turnout_ok,
        "precinct_count_match": precinct_ok,
        "corrupted_race_count": corrupted_count,
        "precinct_count": precinct_count,
        "turnout_detail": turnout_detail,
        "notes": notes,
    }
    return assessment


def assess_all_elections(db_path=None):
    """Run confidence assessment for every election in the database.

    Returns a list of assessment dicts.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM elections ORDER BY election_date")
    election_ids = [row[0] for row in cursor.fetchall()]
    conn.close()

    results = []
    for eid in election_ids:
        assessment = assess_election_confidence(election_id=eid, db_path=db_path)
        results.append(assessment)

    return results


def get_data_quality_summary(db_path=None):
    """Return a DataFrame of all elections with their quality scores.

    Columns: election_date, election_name, election_type,
             overall_confidence, confidence_score, source_type,
             pdf_parsed_ok, cross_validated, race_names_clean,
             turnout_consistent, precinct_count_match, notes
    """
    if not HAS_PANDAS:
        raise ImportError("pandas is required for get_data_quality_summary()")

    conn = get_connection(db_path)
    query = """
        SELECT
            e.election_date,
            e.election_name,
            e.election_type,
            dq.overall_confidence,
            dq.confidence_score,
            dq.source_type,
            dq.pdf_parsed_ok,
            dq.cross_validated,
            dq.race_names_clean,
            dq.turnout_consistent,
            dq.precinct_count_match,
            dq.notes,
            dq.assessed_at
        FROM data_quality dq
        JOIN elections e ON dq.election_id = e.id
        ORDER BY e.election_date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 70)
    print("BCD Data Quality Assessment")
    print("=" * 70)
    print()

    # Ensure the data_quality table exists
    init_db()

    # Assess all elections
    assessments = assess_all_elections()

    # Print summary
    print(f"Assessed {len(assessments)} elections:\n")
    print(f"{'Date':<14} {'Name':<35} {'Score':>6} {'Level':<8} {'Source':<14}")
    print("-" * 80)

    high_count = 0
    medium_count = 0
    low_count = 0

    for a in assessments:
        level = a["overall_confidence"]
        if level == "high":
            high_count += 1
        elif level == "medium":
            medium_count += 1
        else:
            low_count += 1

        name = (a["election_name"] or "Unknown")[:35]
        print(
            f"{a['election_date']:<14} {name:<35} {a['confidence_score']:>5.3f} "
            f"{level:<8} {a['source_type']:<14}"
        )

        # Print flag details for non-high elections
        if level != "high":
            flags = []
            if not a["race_names_clean"]:
                flags.append(f"corrupted_races={a['corrupted_race_count']}")
            if not a["turnout_consistent"]:
                flags.append("turnout_mismatch")
            if not a["cross_validated"]:
                flags.append("not_cross_validated")
            if not a["precinct_count_match"]:
                flags.append(f"precincts={a['precinct_count']}")
            if flags:
                print(f"{'':>14}   Flags: {', '.join(flags)}")

    print()
    print(f"Summary: {high_count} high, {medium_count} medium, {low_count} low")
    print(f"Database: {DB_PATH}")
    print()

    # Also show DataFrame if pandas available
    if HAS_PANDAS:
        df = get_data_quality_summary()
        print("Data quality summary DataFrame:")
        print(df[["election_date", "overall_confidence", "confidence_score", "source_type"]].to_string(index=False))
