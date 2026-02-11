"""
BCD Re-Import Script for Corrupted Elections (2010-2015)
Safely deletes corrupted election data and re-imports from source PDFs.

Steps per election:
  1. Back up existing data counts for verification
  2. Delete all results, races, turnout for that election
  3. Re-parse PDF
  4. Re-import with proper race names and classification
  5. Verify result counts
"""

import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from parse_all_pdfs import parse_pdf_universal, classify_race_level, extract_race_party
from database import get_connection, DB_PATH, init_db, insert_county
from validate_and_fix import improved_classify_race_level


def backup_election_stats(election_date, db_path=None):
    """Get stats about an election before deletion."""
    conn = get_connection(db_path)
    cur = conn.cursor()

    stats = {}
    cur.execute("SELECT id FROM elections WHERE election_date = ?", (election_date,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None
    election_id = row["id"]
    stats["election_id"] = election_id

    cur.execute("SELECT COUNT(*) FROM races WHERE election_id = ?", (election_id,))
    stats["race_count"] = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM results res
        JOIN races r ON res.race_id = r.id
        WHERE r.election_id = ?
    """, (election_id,))
    stats["result_count"] = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM turnout WHERE election_id = ?
    """, (election_id,))
    stats["turnout_count"] = cur.fetchone()[0]

    cur.execute("""
        SELECT SUM(res.votes) FROM results res
        JOIN races r ON res.race_id = r.id
        WHERE r.election_id = ?
    """, (election_id,))
    stats["total_votes"] = cur.fetchone()[0] or 0

    cur.execute("""
        SELECT COUNT(DISTINCT res.precinct_id) FROM results res
        JOIN races r ON res.race_id = r.id
        WHERE r.election_id = ?
    """, (election_id,))
    stats["precinct_count"] = cur.fetchone()[0]

    conn.close()
    return stats


def delete_election_data(election_date, db_path=None):
    """Delete all data for an election (results, races, turnout) but keep the election record."""
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.execute("SELECT id FROM elections WHERE election_date = ?", (election_date,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False
    election_id = row["id"]

    # Delete results first (references races)
    cur.execute("""
        DELETE FROM results WHERE race_id IN (
            SELECT id FROM races WHERE election_id = ?
        )
    """, (election_id,))
    results_deleted = cur.rowcount

    # Delete races
    cur.execute("DELETE FROM races WHERE election_id = ?", (election_id,))
    races_deleted = cur.rowcount

    # Delete turnout
    cur.execute("DELETE FROM turnout WHERE election_id = ?", (election_id,))
    turnout_deleted = cur.rowcount

    # Delete the election record itself so re-import creates a fresh one
    cur.execute("DELETE FROM elections WHERE id = ?", (election_id,))

    # Delete import_log entries for this source
    cur.execute("DELETE FROM import_log WHERE filename LIKE ?", (f"%{election_date[:4]}%",))

    conn.commit()
    conn.close()

    print(f"    Deleted: {results_deleted} results, {races_deleted} races, {turnout_deleted} turnout")
    return True


def reimport_election(election_date, pdf_path, db_path=None):
    """Re-parse and re-import one election from PDF."""
    if db_path is None:
        db_path = DB_PATH

    print(f"\n{'='*60}")
    print(f"RE-IMPORTING: {election_date} from {os.path.basename(pdf_path)}")
    print(f"{'='*60}")

    # Step 1: Backup stats
    old_stats = backup_election_stats(election_date, db_path)
    if old_stats:
        print(f"  OLD: {old_stats['race_count']} races, {old_stats['result_count']} results, {old_stats['precinct_count']} precincts, {old_stats['total_votes']} total votes")
    else:
        print(f"  No existing data found for {election_date}")

    # Step 2: Re-parse PDF
    print(f"  Parsing PDF...")
    parsed = parse_pdf_universal(pdf_path)
    if not parsed:
        print(f"  ERROR: PDF parse failed!")
        return False

    new_races = set(r["race_name"] for r in parsed["results"])
    print(f"  PARSED: {len(parsed['precincts'])} precincts, {len(parsed['results'])} results, {len(new_races)} races")

    # Step 3: Delete old data
    if old_stats:
        print(f"  Deleting old data...")
        delete_election_data(election_date, db_path)

    # Step 4: Re-import with improved classification
    print(f"  Re-importing...")
    records = load_parsed_with_improved_classification(parsed, db_path)

    # Step 5: Verify
    new_stats = backup_election_stats(election_date, db_path)
    if new_stats:
        print(f"  NEW: {new_stats['race_count']} races, {new_stats['result_count']} results, {new_stats['precinct_count']} precincts, {new_stats['total_votes']} total votes")

        if old_stats:
            # Compare
            if new_stats["result_count"] >= old_stats["result_count"] * 0.9:
                print(f"  PASS: Result count looks good ({new_stats['result_count']} vs {old_stats['result_count']} old)")
            else:
                print(f"  WARNING: Fewer results than before ({new_stats['result_count']} vs {old_stats['result_count']} old)")

    return True


def load_parsed_with_improved_classification(parsed_data, db_path=None):
    """Load parsed data into DB using improved race level classification."""
    if parsed_data is None:
        return 0

    init_db(db_path)
    conn = get_connection(db_path)
    cursor = conn.cursor()

    county_id = insert_county("Boone", "IN", db_path=db_path)

    election_date = parsed_data["election_date"]
    election_name = parsed_data["election_name"]
    election_type = parsed_data["election_type"]

    # Check if this election already exists
    cursor.execute(
        "SELECT id FROM elections WHERE county_id = ? AND election_date = ?",
        (county_id, election_date)
    )
    existing = cursor.fetchone()
    if existing:
        print(f"  Election already exists (ID {existing['id']}), skipping.")
        conn.close()
        return 0

    # Create election
    cursor.execute(
        """INSERT INTO elections
           (county_id, election_date, election_type, election_name, source_file)
           VALUES (?, ?, ?, ?, ?)""",
        (county_id, election_date, election_type, election_name, parsed_data["source_file"])
    )
    conn.commit()
    cursor.execute(
        "SELECT id FROM elections WHERE county_id = ? AND election_date = ?",
        (county_id, election_date)
    )
    election_id = cursor.fetchone()["id"]

    # Create precincts and turnout
    precinct_id_map = {}
    for pct in parsed_data["precincts"]:
        pct_name = pct["precinct_name"]
        cursor.execute(
            "INSERT OR IGNORE INTO precincts (county_id, precinct_name, precinct_code) VALUES (?, ?, ?)",
            (county_id, pct_name, pct["precinct_id"])
        )
        conn.commit()
        cursor.execute(
            "SELECT id FROM precincts WHERE county_id = ? AND precinct_name = ?",
            (county_id, pct_name)
        )
        db_pct_id = cursor.fetchone()["id"]
        precinct_id_map[pct["precinct_id"]] = db_pct_id

        if pct.get("registered_voters"):
            cursor.execute(
                """INSERT INTO turnout (election_id, precinct_id, registered_voters, ballots_cast, turnout_percentage)
                   VALUES (?, ?, ?, ?, ?)""",
                (election_id, db_pct_id, pct["registered_voters"], pct.get("public_count"), pct.get("turnout_pct"))
            )

    # Create races, candidates, results — using IMPROVED classifier
    race_cache = {}
    candidate_cache = {}
    records = 0

    for row in parsed_data["results"]:
        race_name = row["race_name"]
        if race_name not in race_cache:
            # Use improved classification!
            race_level = improved_classify_race_level(race_name)
            cursor.execute(
                "INSERT INTO races (election_id, race_name, race_level) VALUES (?, ?, ?)",
                (election_id, race_name, race_level)
            )
            race_cache[race_name] = cursor.lastrowid
        race_id = race_cache[race_name]

        cand_key = (row["candidate_name"], row["party"])
        if cand_key not in candidate_cache:
            cursor.execute(
                "SELECT id FROM candidates WHERE name = ? AND (party = ? OR (party IS NULL AND ? IS NULL))",
                (row["candidate_name"], row["party"], row["party"])
            )
            existing = cursor.fetchone()
            if existing:
                candidate_cache[cand_key] = existing["id"]
            else:
                cursor.execute(
                    "INSERT INTO candidates (name, party) VALUES (?, ?)",
                    (row["candidate_name"], row["party"])
                )
                candidate_cache[cand_key] = cursor.lastrowid
        candidate_id = candidate_cache[cand_key]

        db_pct_id = precinct_id_map.get(row["precinct_id"])

        cursor.execute(
            """INSERT INTO results (race_id, candidate_id, precinct_id, votes, vote_percentage)
               VALUES (?, ?, ?, ?, ?)""",
            (race_id, candidate_id, db_pct_id, row["total_votes"], row.get("vote_percentage"))
        )
        records += 1

    # Log import
    cursor.execute(
        """INSERT INTO import_log (filename, file_type, records_imported, status)
           VALUES (?, 'pdf_reimport', ?, 'success')""",
        (parsed_data["source_file"], records)
    )

    conn.commit()
    conn.close()

    print(f"  Loaded: {records} records, {len(race_cache)} races, {len(candidate_cache)} candidates")
    return records


def run_reimport_all():
    """Re-import all corrupted elections."""
    db_path = DB_PATH

    # Make a backup first
    import shutil
    backup_path = db_path.replace(".db", "_backup_before_reimport.db")
    shutil.copy2(db_path, backup_path)
    print(f"Database backed up to: {backup_path}")

    # Elections to re-import (all with corrupted data)
    elections = {
        "2010-05-04": "/Users/jb/Downloads/2010-Primary-Election-Results.pdf",
        "2010-11-02": "/Users/jb/Downloads/2010-General-Election-Results.pdf",
        # 2011 is scanned — skip
        "2012-05-08": "/Users/jb/Downloads/2012-Primary-Election-Results.pdf",
        "2012-11-06": "/Users/jb/Downloads/2012-General-Election-Results.pdf",
        "2014-05-06": "/Users/jb/Downloads/2014-Primary-Election-Results.pdf",
        "2014-11-04": "/Users/jb/Downloads/2014-General-Election-Results.pdf",
        "2015-05-05": "/Users/jb/Downloads/2015-Primary-Election-Results.pdf",
        "2015-11-03": "/Users/jb/Downloads/2015-General-Election-Results.pdf",
    }

    success = 0
    failed = 0

    for date, pdf_path in sorted(elections.items()):
        if not os.path.exists(pdf_path):
            print(f"\nSKIPPING {date}: PDF not found at {pdf_path}")
            failed += 1
            continue

        try:
            result = reimport_election(date, pdf_path, db_path)
            if result:
                success += 1
            else:
                failed += 1
        except Exception as e:
            print(f"\nERROR re-importing {date}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*60}")
    print(f"RE-IMPORT COMPLETE: {success} succeeded, {failed} failed")
    print(f"Backup at: {backup_path}")

    # Final stats
    conn = get_connection(db_path)
    cur = conn.cursor()
    print(f"\nFinal race level distribution:")
    cur.execute("SELECT race_level, COUNT(*) FROM races GROUP BY race_level ORDER BY COUNT(*) DESC")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")

    cur.execute("SELECT COUNT(*) FROM races WHERE race_level = 'other' AND race_name GLOB '*[0-9] [0-9]*[0-9]%*'")
    print(f"\nRemaining corrupted races: {cur.fetchone()[0]}")
    conn.close()


if __name__ == "__main__":
    run_reimport_all()
