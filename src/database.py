"""
BCD Database Module
Manages the SQLite database for all Boone County election data.
Designed to be reusable for any Indiana county.
"""

import sqlite3
import os
from datetime import datetime


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "bcd_elections.db")


def get_connection(db_path=None):
    """Get a database connection."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path=None):
    """Initialize the database with all required tables."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # -- County table (supports multi-county use) --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS counties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'IN',
            fips_code TEXT,
            clerk_website TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, state)
        )
    """)

    # -- Elections table --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS elections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            county_id INTEGER NOT NULL,
            election_date DATE NOT NULL,
            election_type TEXT NOT NULL,  -- 'general', 'primary', 'special', 'municipal'
            election_name TEXT,           -- e.g. '2024 General Election'
            total_registered_voters INTEGER,
            total_ballots_cast INTEGER,
            source_file TEXT,             -- original PDF/Excel filename
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (county_id) REFERENCES counties(id),
            UNIQUE(county_id, election_date, election_type)
        )
    """)

    # -- Precincts table --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS precincts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            county_id INTEGER NOT NULL,
            precinct_name TEXT NOT NULL,
            precinct_code TEXT,
            township TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (county_id) REFERENCES counties(id),
            UNIQUE(county_id, precinct_name)
        )
    """)

    # -- Races/Contests table --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS races (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            election_id INTEGER NOT NULL,
            race_name TEXT NOT NULL,       -- e.g. 'US President', 'County Commissioner District 1'
            race_level TEXT,               -- 'federal', 'state', 'county', 'municipal', 'school_board'
            race_type TEXT,                -- 'partisan', 'nonpartisan', 'referendum'
            total_votes INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (election_id) REFERENCES elections(id)
        )
    """)

    # -- Candidates table --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            party TEXT,                    -- 'D', 'R', 'L', 'I', etc.
            incumbent INTEGER DEFAULT 0,   -- 1 if incumbent
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # -- Results: the core data table --
    # -- Links races, candidates, and precincts with vote counts --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id INTEGER NOT NULL,
            candidate_id INTEGER NOT NULL,
            precinct_id INTEGER,           -- NULL = county-wide total
            votes INTEGER NOT NULL DEFAULT 0,
            vote_percentage REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (race_id) REFERENCES races(id),
            FOREIGN KEY (candidate_id) REFERENCES candidates(id),
            FOREIGN KEY (precinct_id) REFERENCES precincts(id)
        )
    """)

    # -- Voter registration snapshots over time --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS registration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            county_id INTEGER NOT NULL,
            snapshot_date DATE NOT NULL,
            precinct_id INTEGER,           -- NULL = county-wide
            party TEXT,                    -- 'D', 'R', 'L', 'I', 'unaffiliated'
            registered_count INTEGER NOT NULL,
            source_file TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (county_id) REFERENCES counties(id),
            FOREIGN KEY (precinct_id) REFERENCES precincts(id)
        )
    """)

    # -- Turnout tracking --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS turnout (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            election_id INTEGER NOT NULL,
            precinct_id INTEGER,           -- NULL = county-wide
            registered_voters INTEGER,
            ballots_cast INTEGER,
            turnout_percentage REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (election_id) REFERENCES elections(id),
            FOREIGN KEY (precinct_id) REFERENCES precincts(id)
        )
    """)

    # -- Data import log for tracking what's been loaded --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS import_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            file_type TEXT,               -- 'pdf', 'excel', 'csv'
            records_imported INTEGER,
            status TEXT,                  -- 'success', 'partial', 'failed'
            notes TEXT,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    return True


def insert_county(name, state="IN", fips_code=None, clerk_website=None, db_path=None):
    """Insert or get a county record."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO counties (name, state, fips_code, clerk_website) VALUES (?, ?, ?, ?)",
        (name, state, fips_code, clerk_website)
    )
    conn.commit()
    cursor.execute("SELECT id FROM counties WHERE name = ? AND state = ?", (name, state))
    row = cursor.fetchone()
    conn.close()
    return row["id"]


if __name__ == "__main__":
    print("Initializing BCD database...")
    init_db()
    county_id = insert_county(
        "Boone",
        "IN",
        "18011",
        "https://www.boonecounty.in.gov/department/?structureid=16"
    )
    print(f"Database initialized. Boone County ID: {county_id}")
    print(f"Database location: {DB_PATH}")
