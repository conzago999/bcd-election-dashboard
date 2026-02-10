"""
BCD Boone County PDF Parser
Custom parser for the Boone County, IN Clerk's "Precinct Summary Report" format.

This handles the specific format used by the Boone County election system (EMS).
The format has a consistent structure:
  - Each precinct starts with a header block (precinct ID, name, registered voters, turnout)
  - Followed by race blocks with "VOTE FOR N" header, race name, VOTES=N
  - Each candidate line: E_votes A_votes W_votes TOTAL percentage (Party) Name
  - Precinct ends with straight party votes

This parser extracts ALL of this into structured data.
"""

import pdfplumber
import re
import pandas as pd
import os
import sys
from database import get_connection, init_db, insert_county


def parse_boone_county_pdf(filepath):
    """
    Parse a Boone County Precinct Summary Report PDF.
    Returns a dict with structured data for all precincts.
    """
    all_text = []

    with pdfplumber.open(filepath) as pdf:
        print(f"Reading {len(pdf.pages)} pages...")
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                all_text.append(text)

    full_text = "\n".join(all_text)

    # Extract election metadata from first page
    election_date_match = re.search(r"Election Date:\s*(\d+/\d+/\d+)", full_text)
    election_date = election_date_match.group(1) if election_date_match else "Unknown"

    election_name_match = re.search(r"(\d{4}\s+\w+\s+Election)", full_text)
    election_name = election_name_match.group(1) if election_name_match else "Unknown Election"

    print(f"Election: {election_name} ({election_date})")

    # Split into precinct sections
    # Each precinct starts with its header page containing registration data
    precinct_sections = re.split(
        r"(?=E - # Of Election Day\s+\d+\s+PRECINCT STATUS:)",
        full_text
    )

    precincts = []
    all_results = []

    for section in precinct_sections:
        if "Precinct ID:" not in section:
            continue

        precinct = parse_precinct_section(section)
        if precinct:
            precincts.append(precinct["info"])
            all_results.extend(precinct["results"])

    print(f"Parsed {len(precincts)} precincts, {len(all_results)} result rows")

    return {
        "election_date": election_date,
        "election_name": election_name,
        "precincts": precincts,
        "results": all_results,
    }


def parse_precinct_section(section):
    """Parse a single precinct's section of the report."""

    # Extract precinct info
    pct_id_match = re.search(r"Precinct ID:\s*(\d+)", section)
    pct_name_match = re.search(r"Precinct Name:\s*(.+?)(?:\n|$)", section)
    reg_voters_match = re.search(r"REGISTERED VOTERS:\s*([\d,]+)", section)
    turnout_match = re.search(r"VOTER TURNOUT:\s*([\d.]+)%", section)
    public_count_match = re.search(r"PUBLIC COUNT:\s*([\d,]+)", section)
    election_day_match = re.search(r"E - # Of Election Day\s+(\d+)", section)
    absentee_match = re.search(r"A - # Of Paper Absentee\s+(\d+)", section)
    walkin_match = re.search(r"W - # Of Walk-In Absentee\s+(\d+)", section)

    if not pct_id_match:
        return None

    pct_id = pct_id_match.group(1)
    pct_name = pct_name_match.group(1).strip() if pct_name_match else f"Precinct {pct_id}"

    info = {
        "precinct_id": pct_id,
        "precinct_name": pct_name,
        "registered_voters": int(reg_voters_match.group(1).replace(",", "")) if reg_voters_match else None,
        "turnout_pct": float(turnout_match.group(1)) if turnout_match else None,
        "public_count": int(public_count_match.group(1).replace(",", "")) if public_count_match else None,
        "election_day_votes": int(election_day_match.group(1)) if election_day_match else None,
        "absentee_votes": int(absentee_match.group(1)) if absentee_match else None,
        "walkin_votes": int(walkin_match.group(1)) if walkin_match else None,
    }

    # Parse race results
    results = []

    # Find all race blocks: "VOTE FOR N\nRace Name\nVOTES=N"
    race_pattern = re.compile(
        r"VOTE FOR (\d+)\n(.+?)\nVOTES=(\d+)\n((?:.*?\n)*?)(?=VOTE FOR \d+|Straight Party Votes|Precinct Summary Report)",
        re.MULTILINE
    )

    for match in race_pattern.finditer(section):
        vote_for = int(match.group(1))
        race_name = match.group(2).strip()
        total_race_votes = int(match.group(3))
        candidates_block = match.group(4)

        # Parse each candidate line
        # Format: E A W TOTAL PCT% (PARTY) Name  OR  E A W TOTAL PCT% Yes/No/Write-In
        cand_pattern = re.compile(
            r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)%\s+(.+)"
        )

        for cand_match in cand_pattern.finditer(candidates_block):
            e_votes = int(cand_match.group(1))
            a_votes = int(cand_match.group(2))
            w_votes = int(cand_match.group(3))
            total_votes = int(cand_match.group(4))
            pct = float(cand_match.group(5))
            name_str = cand_match.group(6).strip()

            # Parse party from name: "(R) Name" or "(D) Name" etc
            party = None
            candidate_name = name_str
            party_match = re.match(r"\((\w+)\)\s+(.+)", name_str)
            if party_match:
                party_code = party_match.group(1)
                candidate_name = party_match.group(2).strip()
                # Normalize party codes
                party_map = {
                    "R": "R", "REP": "R",
                    "D": "D", "DEM": "D",
                    "L": "L", "LIB": "L",
                    "WTP": "WTP",
                    "NP": "NP",  # Nonpartisan
                }
                party = party_map.get(party_code, party_code)
            elif name_str in ("Yes", "No"):
                candidate_name = name_str
                party = None
            elif name_str == "Write-In":
                candidate_name = "Write-In"
                party = None

            results.append({
                "precinct_id": pct_id,
                "precinct_name": pct_name,
                "race_name": race_name,
                "vote_for": vote_for,
                "candidate_name": candidate_name,
                "party": party,
                "election_day_votes": e_votes,
                "absentee_votes": a_votes,
                "walkin_votes": w_votes,
                "total_votes": total_votes,
                "vote_percentage": pct,
            })

    # Parse straight party votes at the end
    straight_party_match = re.findall(
        r"(Democratic Party|Republican Party|Libertarian Party)\s+(\d+)",
        section
    )
    for party_name, votes in straight_party_match:
        party_code = {"Democratic Party": "D", "Republican Party": "R", "Libertarian Party": "L"}.get(party_name, party_name)
        results.append({
            "precinct_id": pct_id,
            "precinct_name": pct_name,
            "race_name": "Straight Party",
            "vote_for": 1,
            "candidate_name": party_name,
            "party": party_code,
            "election_day_votes": None,
            "absentee_votes": None,
            "walkin_votes": None,
            "total_votes": int(votes),
            "vote_percentage": None,
        })

    return {"info": info, "results": results}


def parsed_to_dataframes(parsed_data):
    """Convert parsed data to pandas DataFrames for easy analysis."""
    precincts_df = pd.DataFrame(parsed_data["precincts"])
    results_df = pd.DataFrame(parsed_data["results"])

    return precincts_df, results_df


def load_into_database(parsed_data, county_name="Boone", state="IN", db_path=None):
    """Load parsed PDF data into the BCD database."""
    init_db(db_path)
    conn = get_connection(db_path)
    cursor = conn.cursor()

    county_id = insert_county(county_name, state, db_path=db_path)

    # Parse election date
    date_str = parsed_data["election_date"]
    # Convert MM/DD/YYYY to YYYY-MM-DD
    parts = date_str.split("/")
    if len(parts) == 3:
        election_date = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
    else:
        election_date = date_str

    election_name = parsed_data["election_name"]
    election_type = "general" if "general" in election_name.lower() else \
                    "primary" if "primary" in election_name.lower() else \
                    "special" if "special" in election_name.lower() else "other"

    # Create election
    cursor.execute(
        """INSERT OR IGNORE INTO elections
           (county_id, election_date, election_type, election_name, source_file)
           VALUES (?, ?, ?, ?, ?)""",
        (county_id, election_date, election_type, election_name, "allprecinctsafterprov.pdf")
    )
    conn.commit()
    cursor.execute(
        "SELECT id FROM elections WHERE county_id = ? AND election_date = ? AND election_type = ?",
        (county_id, election_date, election_type)
    )
    election_id = cursor.fetchone()["id"]

    # Create precincts and store turnout
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

        # Store turnout data
        if pct["registered_voters"]:
            cursor.execute(
                """INSERT INTO turnout (election_id, precinct_id, registered_voters, ballots_cast, turnout_percentage)
                   VALUES (?, ?, ?, ?, ?)""",
                (election_id, db_pct_id, pct["registered_voters"], pct["public_count"], pct["turnout_pct"])
            )

    # Create races, candidates, and results
    race_cache = {}
    candidate_cache = {}
    records = 0

    for row in parsed_data["results"]:
        # Get or create race
        race_name = row["race_name"]
        if race_name not in race_cache:
            # Determine race level
            race_level = classify_race_level(race_name)
            cursor.execute(
                "INSERT INTO races (election_id, race_name, race_level) VALUES (?, ?, ?)",
                (election_id, race_name, race_level)
            )
            race_cache[race_name] = cursor.lastrowid
        race_id = race_cache[race_name]

        # Get or create candidate
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

        # Get precinct DB id
        db_pct_id = precinct_id_map.get(row["precinct_id"])

        # Insert result
        cursor.execute(
            """INSERT INTO results (race_id, candidate_id, precinct_id, votes, vote_percentage)
               VALUES (?, ?, ?, ?, ?)""",
            (race_id, candidate_id, db_pct_id, row["total_votes"], row["vote_percentage"])
        )
        records += 1

    # Log import
    cursor.execute(
        """INSERT INTO import_log (filename, file_type, records_imported, status)
           VALUES (?, 'pdf', ?, 'success')""",
        (os.path.basename(parsed_data.get("source_file", "allprecinctsafterprov.pdf")), records)
    )

    conn.commit()
    conn.close()

    print(f"\nLoaded into database:")
    print(f"  Election: {election_name} ({election_date})")
    print(f"  Precincts: {len(parsed_data['precincts'])}")
    print(f"  Races: {len(race_cache)}")
    print(f"  Candidates: {len(candidate_cache)}")
    print(f"  Result records: {records}")

    return records


def classify_race_level(race_name):
    """Classify a race into federal/state/county/municipal/school_board level."""
    name_lower = race_name.lower()
    if any(kw in name_lower for kw in ["president", "united states senator", "united states representative"]):
        return "federal"
    if any(kw in name_lower for kw in ["governor", "attorney general", "state senator", "state representative",
                                        "supreme court", "court of appeals"]):
        return "state"
    if any(kw in name_lower for kw in ["county", "circuit court", "coroner", "commissioner", "council",
                                        "auditor", "recorder", "treasurer"]):
        return "county"
    if any(kw in name_lower for kw in ["school", "community school"]):
        return "school_board"
    if any(kw in name_lower for kw in ["public question", "const amendment", "straight party"]):
        return "ballot_measure"
    return "other"


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python parse_boone_pdf.py <pdf_file>            Parse and show results")
        print("  python parse_boone_pdf.py <pdf_file> --load     Parse and load into database")
        print("  python parse_boone_pdf.py <pdf_file> --export   Parse and export to Excel")
        sys.exit(1)

    filepath = sys.argv[1]
    print(f"Parsing: {filepath}")
    parsed = parse_boone_county_pdf(filepath)
    precincts_df, results_df = parsed_to_dataframes(parsed)

    print(f"\n{'='*60}")
    print(f"PARSING SUMMARY")
    print(f"{'='*60}")
    print(f"Election: {parsed['election_name']} ({parsed['election_date']})")
    print(f"Precincts: {len(precincts_df)}")
    print(f"Total result rows: {len(results_df)}")
    print(f"\nPrecincts found:")
    print(precincts_df[["precinct_id", "precinct_name", "registered_voters", "turnout_pct"]].to_string(index=False))
    print(f"\nRaces found:")
    print(results_df["race_name"].unique())
    print(f"\nSample results (first 20):")
    print(results_df.head(20).to_string(index=False))

    if "--load" in sys.argv:
        print(f"\n{'='*60}")
        print("Loading into database...")
        load_into_database(parsed)
        print("Done!")

    if "--export" in sys.argv:
        output = os.path.splitext(filepath)[0] + "_parsed.xlsx"
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            precincts_df.to_excel(writer, sheet_name="Precincts", index=False)
            results_df.to_excel(writer, sheet_name="Results", index=False)
        print(f"\nExported to: {output}")
