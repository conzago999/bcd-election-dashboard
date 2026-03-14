"""
ETL: Ingest Indiana ENR CSV files into the multi-county SQLite database.

Reads all CSV files from data/state_research/ and loads them into
data/indiana_elections.db. Handles both "Precinct" and "Locality" level data,
normalizes 2018 header format, and maps party names.

Usage:
    python v2/etl.py
"""

import csv
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from database import get_connection, init_db, DB_PATH

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "state_research")

# Map ENR party names to short codes
PARTY_MAP = {
    "Democratic": "D",
    "Republican": "R",
    "Libertarian": "L",
    "Independent": "I",
    "Green": "G",
    "American Solidarity": "AS",
    "Constitution": "C",
    "Political Synergy": "PS",
    "Other": "O",
}

# Classify office categories into levels
OFFICE_LEVEL_MAP = {
    "Presidential Electors For US President & Vp": "federal",
    "US Senator": "federal",
    "US Representative": "federal",
    "Governor & Lt. Governor": "state",
    "Attorney General": "state",
    "State Senator": "state",
    "State Representative": "state",
    "Judge, Circuit Court": "judicial",
    "Judge, Superior Court": "judicial",
    "Judge, Probate Court": "judicial",
    "County Commissioner": "county",
    "County Council Member": "county",
    "County Auditor": "county",
    "County Treasurer": "county",
    "County Recorder": "county",
    "County Surveyor": "county",
    "County Coroner": "county",
    "County Sheriff": "county",
    "County Assessor": "county",
    "Clerk Of The Circuit Court": "county",
    "City-County Or City Common Council Member": "municipal",
    "Mayor": "municipal",
    "City Clerk-Treasurer": "municipal",
    "Town Council Member": "municipal",
    "Township Trustee": "township",
    "Township Board Member": "township",
    "School Board Member": "school_board",
}


def classify_race_level(office_category):
    """Map an office category string to a race level."""
    if not office_category:
        return "other"
    for keyword, level in OFFICE_LEVEL_MAP.items():
        if keyword.lower() in office_category.lower():
            return level
    return "other"


def map_party(party_str):
    """Map full party name to short code."""
    if not party_str:
        return "O"
    for full, short in PARTY_MAP.items():
        if full.lower() in party_str.lower():
            return short
    return "O"


def parse_election_info(election_name):
    """Extract date and type from election name like '2024 General Election'."""
    m = re.match(r"(\d{4})\s+(General|Primary|Municipal|Special)", election_name, re.IGNORECASE)
    if m:
        year = m.group(1)
        etype = m.group(2).lower()
        # Approximate dates
        if etype == "general":
            return f"{year}-11-05", etype
        elif etype == "primary":
            return f"{year}-05-07", etype
        elif etype == "municipal":
            return f"{year}-11-05", etype
        else:
            return f"{year}-01-01", etype
    return None, "unknown"


def normalize_headers(headers):
    """Normalize 2018-style space-separated headers to camelCase."""
    mapping = {
        "Election": "Election",
        "Jurisdiction Name": "JurisdictionName",
        "Reporting County Name": "ReportingCountyName",
        "DataEntry Jurisdiction Name": "DataEntryJurisdictionName",
        "DataEntry Level Name": "DataEntryLevelName",
        "Office": "Office",
        "Office Category": "OfficeCategory",
        "Ballot Order": "BallotOrder",
        "Name on Ballot": "NameonBallot",
        "Political Party": "PoliticalParty",
        "Winner": "Winner",
        "Number of Office Seats": "NumberofOfficeSeats",
        "Total Votes": "TotalVotes",
    }
    return [mapping.get(h, h) for h in headers]


def ingest_csv(filepath, conn):
    """Ingest a single ENR CSV file into the database."""
    filename = os.path.basename(filepath)
    print(f"  Reading {filename}...")

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        raw_headers = next(reader)
        # Strip quotes and whitespace
        raw_headers = [h.strip().strip('"') for h in raw_headers]
        headers = normalize_headers(raw_headers)

        rows = []
        for row in reader:
            if len(row) == len(headers):
                rows.append(dict(zip(headers, [v.strip().strip('"') for v in row])))

    if not rows:
        print(f"    Skipping {filename} — no data rows")
        return 0

    c = conn.cursor()

    # Cache lookups to avoid repeated queries
    county_cache = {}
    election_cache = {}
    precinct_cache = {}
    race_cache = {}

    inserted = 0
    for row in rows:
        election_name = row.get("Election", "")
        county_name = row.get("ReportingCountyName", "")
        precinct_name = row.get("DataEntryJurisdictionName", "")
        data_level = row.get("DataEntryLevelName", "")
        office = row.get("Office", "")
        office_cat = row.get("OfficeCategory", "")
        candidate = row.get("NameonBallot", "")
        party = row.get("PoliticalParty", "")
        winner = row.get("Winner", "")
        num_seats = row.get("NumberofOfficeSeats", "1")
        total_votes = row.get("TotalVotes", "0")

        if not county_name or not office or not candidate:
            continue

        # Parse votes
        try:
            votes = int(total_votes)
        except (ValueError, TypeError):
            votes = 0

        # Get or create county
        if county_name not in county_cache:
            c.execute("INSERT OR IGNORE INTO counties (name) VALUES (?)", (county_name,))
            c.execute("SELECT id FROM counties WHERE name = ?", (county_name,))
            county_cache[county_name] = c.fetchone()[0]
        county_id = county_cache[county_name]

        # Get or create election
        if election_name not in election_cache:
            edate, etype = parse_election_info(election_name)
            c.execute(
                "INSERT OR IGNORE INTO elections (election_name, election_date, election_type) VALUES (?, ?, ?)",
                (election_name, edate, etype),
            )
            c.execute("SELECT id FROM elections WHERE election_name = ?", (election_name,))
            election_cache[election_name] = c.fetchone()[0]
        election_id = election_cache[election_name]

        # Get or create precinct (only for precinct-level data)
        precinct_id = None
        if data_level == "Precinct" and precinct_name:
            pkey = (county_id, precinct_name)
            if pkey not in precinct_cache:
                c.execute(
                    "INSERT OR IGNORE INTO precincts (county_id, precinct_name) VALUES (?, ?)",
                    (county_id, precinct_name),
                )
                c.execute(
                    "SELECT id FROM precincts WHERE county_id = ? AND precinct_name = ?",
                    (county_id, precinct_name),
                )
                precinct_cache[pkey] = c.fetchone()[0]
            precinct_id = precinct_cache[pkey]

        # Get or create race
        rkey = (election_id, county_id, office)
        if rkey not in race_cache:
            race_level = classify_race_level(office_cat)
            try:
                seats = int(num_seats)
            except (ValueError, TypeError):
                seats = 1
            c.execute(
                "INSERT OR IGNORE INTO races (election_id, county_id, race_name, office_category, race_level, num_seats) VALUES (?, ?, ?, ?, ?, ?)",
                (election_id, county_id, office, office_cat, race_level, seats),
            )
            c.execute(
                "SELECT id FROM races WHERE election_id = ? AND county_id = ? AND race_name = ?",
                (election_id, county_id, office),
            )
            race_cache[rkey] = c.fetchone()[0]
        race_id = race_cache[rkey]

        # Insert result
        party_code = map_party(party)
        is_winner = 1 if winner.lower() == "yes" else 0
        c.execute(
            "INSERT INTO results (race_id, precinct_id, candidate_name, party, votes, winner, data_level) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (race_id, precinct_id, candidate, party_code, votes, is_winner, data_level),
        )
        inserted += 1

    conn.commit()
    print(f"    Inserted {inserted:,} result rows")
    return inserted


def update_county_stats(conn):
    """Update county metadata (precinct counts, etc.)."""
    c = conn.cursor()
    c.execute("""
        UPDATE counties SET
            has_precinct_data = (
                SELECT COUNT(DISTINCT p.id) > 0
                FROM precincts p WHERE p.county_id = counties.id
            ),
            precinct_election_count = (
                SELECT COUNT(DISTINCT r.election_id)
                FROM races r
                JOIN results res ON res.race_id = r.id
                WHERE r.county_id = counties.id AND res.data_level = 'Precinct'
            )
    """)
    conn.commit()


def run_etl():
    """Main ETL entry point."""
    print("=" * 60)
    print("Indiana Election Data ETL")
    print("=" * 60)

    # Find all CSV files
    csv_files = sorted(
        [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")],
        key=lambda x: x,
    )
    if not csv_files:
        print(f"No CSV files found in {DATA_DIR}")
        return

    print(f"Found {len(csv_files)} CSV files in {DATA_DIR}")

    # Initialize database
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing database")
    init_db()
    print(f"Created fresh database at {DB_PATH}")

    conn = get_connection()
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = -64000")

    total = 0
    for csv_file in csv_files:
        filepath = os.path.join(DATA_DIR, csv_file)
        total += ingest_csv(filepath, conn)

    # Update stats
    update_county_stats(conn)

    # Print summary
    c = conn.cursor()
    stats = {
        "counties": c.execute("SELECT COUNT(*) FROM counties").fetchone()[0],
        "elections": c.execute("SELECT COUNT(*) FROM elections").fetchone()[0],
        "precincts": c.execute("SELECT COUNT(*) FROM precincts").fetchone()[0],
        "races": c.execute("SELECT COUNT(*) FROM races").fetchone()[0],
        "results": c.execute("SELECT COUNT(*) FROM results").fetchone()[0],
        "precinct_counties": c.execute(
            "SELECT COUNT(*) FROM counties WHERE has_precinct_data = 1"
        ).fetchone()[0],
    }

    print("\n" + "=" * 60)
    print("ETL COMPLETE")
    print("=" * 60)
    print(f"  Counties:           {stats['counties']}")
    print(f"    w/ precinct data: {stats['precinct_counties']}")
    print(f"  Elections:          {stats['elections']}")
    print(f"  Precincts:          {stats['precincts']}")
    print(f"  Races:              {stats['races']}")
    print(f"  Result rows:        {stats['results']:,}")
    print(f"  Database size:      {os.path.getsize(DB_PATH) / 1024 / 1024:.1f} MB")

    conn.close()


if __name__ == "__main__":
    run_etl()
