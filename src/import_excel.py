"""
BCD Excel/CSV Import Module
Imports election data from Excel spreadsheets and CSV files into the database.
Handles various formats that county clerks and volunteers might produce.
"""

import pandas as pd
import sqlite3
import os
import sys
from datetime import datetime
from database import get_connection, init_db, insert_county, DB_PATH


def preview_file(filepath):
    """Preview the structure of an Excel or CSV file to understand its format."""
    ext = os.path.splitext(filepath)[1].lower()

    if ext in [".xlsx", ".xls"]:
        # Show all sheet names
        xl = pd.ExcelFile(filepath)
        print(f"File: {os.path.basename(filepath)}")
        print(f"Sheets: {xl.sheet_names}")
        print()

        for sheet in xl.sheet_names:
            df = pd.read_excel(filepath, sheet_name=sheet, nrows=10)
            print(f"--- Sheet: '{sheet}' ---")
            print(f"  Columns ({len(df.columns)}): {list(df.columns)}")
            print(f"  Rows (showing first 10):")
            print(df.to_string(index=False))
            print()

    elif ext == ".csv":
        df = pd.read_csv(filepath, nrows=10)
        print(f"File: {os.path.basename(filepath)}")
        print(f"Columns ({len(df.columns)}): {list(df.columns)}")
        print(f"Rows (showing first 10):")
        print(df.to_string(index=False))

    else:
        print(f"Unsupported file type: {ext}")

    return df


def detect_columns(df):
    """
    Attempt to auto-detect which columns map to our schema fields.
    Returns a mapping dict.
    """
    col_lower = {c: c.lower().strip() for c in df.columns}
    mapping = {}

    # Common patterns for election data columns
    patterns = {
        "race": ["race", "contest", "office", "position"],
        "candidate": ["candidate", "name", "cand"],
        "party": ["party", "pty", "affiliation"],
        "votes": ["votes", "vote", "total", "count", "ballots"],
        "precinct": ["precinct", "pct", "ward", "district"],
        "election_date": ["date", "election date", "elec date"],
        "election_type": ["type", "election type", "elec type"],
        "percentage": ["percent", "pct", "%", "vote %", "vote_pct"],
    }

    for field, keywords in patterns.items():
        for col, col_low in col_lower.items():
            if any(kw in col_low for kw in keywords):
                if field not in mapping:
                    mapping[field] = col
                break

    return mapping


def import_election_results(filepath, county_name="Boone", state="IN",
                            election_date=None, election_type=None,
                            sheet_name=0, column_mapping=None, db_path=None):
    """
    Import election results from an Excel/CSV file.

    Parameters:
        filepath: Path to the Excel or CSV file
        county_name: County name (default Boone)
        state: State abbreviation (default IN)
        election_date: Date string like '2024-11-05'
        election_type: 'general', 'primary', 'special', 'municipal'
        sheet_name: Excel sheet name or index
        column_mapping: Dict mapping our fields to actual column names
                       e.g. {'race': 'Contest Name', 'candidate': 'Candidate', ...}
        db_path: Optional database path override
    """
    # Read the file
    ext = os.path.splitext(filepath)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(filepath, sheet_name=sheet_name)
    elif ext == ".csv":
        df = pd.read_csv(filepath)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    print(f"Loaded {len(df)} rows from {os.path.basename(filepath)}")

    # Auto-detect columns if no mapping provided
    if column_mapping is None:
        column_mapping = detect_columns(df)
        print(f"Auto-detected column mapping: {column_mapping}")
        if not column_mapping:
            print("ERROR: Could not auto-detect columns. Please provide a column_mapping.")
            print(f"Available columns: {list(df.columns)}")
            return None

    # Initialize database and get county
    init_db(db_path)
    conn = get_connection(db_path)
    cursor = conn.cursor()
    county_id = insert_county(county_name, state, db_path=db_path)

    # Create election record
    if election_date is None:
        if "election_date" in column_mapping and column_mapping["election_date"] in df.columns:
            election_date = str(df[column_mapping["election_date"]].iloc[0])
        else:
            election_date = input("Election date (YYYY-MM-DD): ")

    if election_type is None:
        if "election_type" in column_mapping and column_mapping["election_type"] in df.columns:
            election_type = str(df[column_mapping["election_type"]].iloc[0]).lower()
        else:
            election_type = input("Election type (general/primary/special/municipal): ")

    election_name = f"{election_date[:4]} {election_type.title()} Election"

    cursor.execute(
        """INSERT OR IGNORE INTO elections (county_id, election_date, election_type, election_name, source_file)
           VALUES (?, ?, ?, ?, ?)""",
        (county_id, election_date, election_type, election_name, os.path.basename(filepath))
    )
    conn.commit()
    cursor.execute(
        "SELECT id FROM elections WHERE county_id = ? AND election_date = ? AND election_type = ?",
        (county_id, election_date, election_type)
    )
    election_id = cursor.fetchone()["id"]

    # Cache for races, candidates, precincts to avoid repeated lookups
    race_cache = {}
    candidate_cache = {}
    precinct_cache = {}
    records_imported = 0

    for _, row in df.iterrows():
        # Get or create race
        race_name = str(row.get(column_mapping.get("race", ""), "Unknown Race"))
        if race_name not in race_cache:
            cursor.execute(
                "INSERT INTO races (election_id, race_name) VALUES (?, ?)",
                (election_id, race_name)
            )
            race_cache[race_name] = cursor.lastrowid
        race_id = race_cache[race_name]

        # Get or create candidate
        cand_name = str(row.get(column_mapping.get("candidate", ""), "Unknown"))
        party = str(row.get(column_mapping.get("party", ""), "")) if "party" in column_mapping else None
        cand_key = (cand_name, party)
        if cand_key not in candidate_cache:
            cursor.execute(
                "SELECT id FROM candidates WHERE name = ? AND (party = ? OR (party IS NULL AND ? IS NULL))",
                (cand_name, party, party)
            )
            existing = cursor.fetchone()
            if existing:
                candidate_cache[cand_key] = existing["id"]
            else:
                cursor.execute(
                    "INSERT INTO candidates (name, party) VALUES (?, ?)",
                    (cand_name, party)
                )
                candidate_cache[cand_key] = cursor.lastrowid
        candidate_id = candidate_cache[cand_key]

        # Get or create precinct (if precinct data exists)
        precinct_id = None
        if "precinct" in column_mapping and column_mapping["precinct"] in df.columns:
            pct_name = str(row[column_mapping["precinct"]])
            if pct_name and pct_name != "nan":
                if pct_name not in precinct_cache:
                    cursor.execute(
                        "INSERT OR IGNORE INTO precincts (county_id, precinct_name) VALUES (?, ?)",
                        (county_id, pct_name)
                    )
                    conn.commit()
                    cursor.execute(
                        "SELECT id FROM precincts WHERE county_id = ? AND precinct_name = ?",
                        (county_id, pct_name)
                    )
                    precinct_cache[pct_name] = cursor.fetchone()["id"]
                precinct_id = precinct_cache[pct_name]

        # Get vote count
        votes = 0
        if "votes" in column_mapping and column_mapping["votes"] in df.columns:
            try:
                votes = int(row[column_mapping["votes"]])
            except (ValueError, TypeError):
                votes = 0

        # Get percentage
        pct = None
        if "percentage" in column_mapping and column_mapping["percentage"] in df.columns:
            try:
                pct = float(row[column_mapping["percentage"]])
            except (ValueError, TypeError):
                pct = None

        # Insert result
        cursor.execute(
            """INSERT INTO results (race_id, candidate_id, precinct_id, votes, vote_percentage)
               VALUES (?, ?, ?, ?, ?)""",
            (race_id, candidate_id, precinct_id, votes, pct)
        )
        records_imported += 1

    # Log the import
    cursor.execute(
        """INSERT INTO import_log (filename, file_type, records_imported, status)
           VALUES (?, ?, ?, 'success')""",
        (os.path.basename(filepath), ext.replace(".", ""), records_imported)
    )

    conn.commit()
    conn.close()

    print(f"Successfully imported {records_imported} result records.")
    print(f"  Races: {len(race_cache)}")
    print(f"  Candidates: {len(candidate_cache)}")
    print(f"  Precincts: {len(precinct_cache)}")

    return records_imported


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python import_excel.py <path_to_file>")
        print("       python import_excel.py <path_to_file> --preview")
        sys.exit(1)

    filepath = sys.argv[1]

    if "--preview" in sys.argv:
        preview_file(filepath)
    else:
        print(f"Previewing file structure first...")
        preview_file(filepath)
        print("\n" + "="*60)
        proceed = input("Proceed with import? (y/n): ")
        if proceed.lower() == "y":
            import_election_results(filepath)
