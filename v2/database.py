"""
Indiana Election Database — V2 Multi-County Schema
Stores precinct-level election results for all 92 Indiana counties.
Data sourced from IN Secretary of State ENR archive CSVs.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "indiana_elections.db")


def get_connection(db_path=None):
    """Get a database connection."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(db_path=None):
    """Initialize the multi-county database."""
    conn = get_connection(db_path)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS counties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            has_precinct_data INTEGER DEFAULT 0,
            precinct_election_count INTEGER DEFAULT 0
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS elections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            election_name TEXT NOT NULL,
            election_date DATE,
            election_type TEXT NOT NULL,
            UNIQUE(election_name)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS precincts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            county_id INTEGER NOT NULL,
            precinct_name TEXT NOT NULL,
            FOREIGN KEY (county_id) REFERENCES counties(id),
            UNIQUE(county_id, precinct_name)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS races (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            election_id INTEGER NOT NULL,
            county_id INTEGER NOT NULL,
            race_name TEXT NOT NULL,
            office_category TEXT,
            race_level TEXT,
            num_seats INTEGER DEFAULT 1,
            FOREIGN KEY (election_id) REFERENCES elections(id),
            FOREIGN KEY (county_id) REFERENCES counties(id),
            UNIQUE(election_id, county_id, race_name)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id INTEGER NOT NULL,
            precinct_id INTEGER,
            candidate_name TEXT NOT NULL,
            party TEXT,
            votes INTEGER NOT NULL DEFAULT 0,
            winner INTEGER DEFAULT 0,
            data_level TEXT NOT NULL DEFAULT 'Precinct',
            FOREIGN KEY (race_id) REFERENCES races(id),
            FOREIGN KEY (precinct_id) REFERENCES precincts(id)
        )
    """)

    # Indexes for common queries
    c.execute("CREATE INDEX IF NOT EXISTS idx_results_race ON results(race_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_results_precinct ON results(precinct_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_races_county ON races(county_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_races_election ON races(election_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_precincts_county ON precincts(county_id)")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
