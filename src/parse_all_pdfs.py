"""
BCD Universal PDF Parser
Handles all format variants from the Boone County Clerk's office across years.

Format variants detected:
  A) 2024/2023 style: "E A W TOTAL %" with "Precinct ID:" / "Precinct Name:" headers
  B) 2022 style: "E A W TOTAL %" with "01-Center 01" combined precinct line
  C) 2016/2014/2018/2012 style: "M A P TOTAL %" with "Precinct ID:" / "Precinct Name:" headers
  D) Summary-only (no precinct breakdown): county-wide totals only

All text-based PDFs from the Boone County clerk follow one of these patterns.
"""

import pdfplumber
import re
import pandas as pd
import os
import sys
from database import get_connection, init_db, insert_county


def detect_format(text):
    """Detect which format variant a PDF uses."""
    if "E - # Of Election Day" in text and "Precinct ID:" in text:
        return "A"  # 2024 style with separate Precinct ID/Name
    if "E - # Of Election Day" in text:
        return "B"  # 2022/2019+ style with E A W columns
    # Format C: older M A P style
    has_machine = bool(re.search(r"M\s*-?\s*#\s*(?:Of|OF)\s*Machine", text))
    if has_machine and "Precinct ID:" in text:
        return "C1"  # 2016/2018 style: M A P with separate Precinct ID/Name
    if has_machine and re.search(r"\d+-\w+", text):
        return "C2"  # 2010/2012/2014 style: M A P with combined "01-Center 1" line
    if has_machine:
        return "C2"  # Default older M A P format
    if "Election Summary Report" in text and "Precinct" not in text:
        return "D"  # Summary only
    return "unknown"


def parse_pdf_universal(filepath):
    """
    Universal parser for all Boone County election PDF formats.
    Detects format and dispatches to appropriate parser.
    """
    all_text = []
    with pdfplumber.open(filepath) as pdf:
        num_pages = len(pdf.pages)
        print(f"  Reading {num_pages} pages...")
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                all_text.append(text)

    if not all_text:
        print(f"  WARNING: No text extracted - likely a scanned PDF")
        return None

    full_text = "\n".join(all_text)

    # Detect format
    fmt = detect_format(full_text)
    print(f"  Detected format: {fmt}")

    # Extract election metadata
    election_date = extract_election_date(full_text)
    election_name = extract_election_name(full_text)
    election_type = classify_election_type(election_name)

    print(f"  Election: {election_name} ({election_date}) [{election_type}]")

    if fmt in ("A", "B", "C1", "C2"):
        result = parse_precinct_report(full_text, fmt, election_date, election_name, election_type, filepath)
        if result and len(result["results"]) > 0:
            return result
        # If precinct parse found nothing, try summary
        print(f"  Precinct parse returned 0 results, trying summary parse...")
        return parse_summary_report(full_text, election_date, election_name, election_type, filepath)
    elif fmt == "D":
        return parse_summary_report(full_text, election_date, election_name, election_type, filepath)
    else:
        # Try to parse as precinct report anyway
        print(f"  Unknown format, attempting precinct parse...")
        result = parse_precinct_report(full_text, "A", election_date, election_name, election_type, filepath)
        if result and len(result["results"]) > 0:
            return result
        # Fallback to summary
        return parse_summary_report(full_text, election_date, election_name, election_type, filepath)


def extract_election_date(text):
    """Extract election date from PDF text."""
    match = re.search(r"Election Date:\s*(\d+/\d+/\d+)", text)
    if match:
        parts = match.group(1).split("/")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
    return "Unknown"


def extract_election_name(text):
    """Extract election name from PDF text."""
    match = re.search(r"(\d{4}\s+(?:General|Primary|Special|Municipal)\s+Election)", text, re.IGNORECASE)
    if match:
        return match.group(1)
    return "Unknown Election"


def classify_election_type(name):
    name_lower = name.lower()
    if "general" in name_lower:
        return "general"
    if "primary" in name_lower:
        return "primary"
    if "special" in name_lower:
        return "special"
    if "municipal" in name_lower:
        return "municipal"
    return "other"


def parse_precinct_report(full_text, fmt, election_date, election_name, election_type, filepath):
    """Parse a precinct-level report (formats A, B, C1, C2)."""

    precincts = []
    all_results = []

    if fmt == "C2":
        # C2 format (2010/2012/2014): PDFs contain "Election Summary Report" pages
        # followed by "Precinct Summary Report" pages. We must strip the summary
        # pages to avoid mixing county totals into precinct data.
        #
        # Each precinct spans multiple pages, all with the same precinct ID line
        # (e.g., "01-Center 1") after the header. We group pages by precinct.

        # Strip Election Summary Report pages - only keep Precinct Summary Report pages
        if "Precinct Summary Report" in full_text:
            # Split at first occurrence of "Precinct Summary Report"
            parts = full_text.split("Precinct Summary Report", 1)
            precinct_text = "Precinct Summary Report" + parts[1]
        else:
            precinct_text = full_text

        # Split into per-page chunks by "Precinct Summary Report" header
        page_chunks = re.split(r"(?=Precinct Summary Report\n)", precinct_text)

        # Group pages by precinct ID (each precinct spans ~3 pages)
        precinct_pages = {}
        for chunk in page_chunks:
            if len(chunk.strip()) < 50:
                continue
            # Extract precinct ID from the line after "Election Date: ..."
            pct_match = re.search(r"Election Date:\s*\d+/\d+/\d+\n(\d{2})-(.+?)(?:\n|$)", chunk)
            if pct_match:
                pct_id = pct_match.group(1)
                if pct_id not in precinct_pages:
                    precinct_pages[pct_id] = []
                precinct_pages[pct_id].append(chunk)

        # Now parse each precinct's combined text
        for pct_id in sorted(precinct_pages.keys()):
            combined_section = "\n".join(precinct_pages[pct_id])

            # Strip repeated page headers and footers from combined text
            # so that race block regex can match cleanly across pages.
            # Remove "Precinct Summary Report\n...Election Date:...\nNN-Name\n...VOTES...\nM A P TOTAL %"
            combined_section = re.sub(
                r"Precinct Summary Report\n.*?M\s+A\s+P\s+TOTAL\s+%\n",
                "",
                combined_section,
                flags=re.DOTALL
            )
            # Remove page footer lines like "INBOOG12 Page 1 of 3"
            combined_section = re.sub(r"INB\w+\s+Page \d+ of \d+", "", combined_section)

            # Re-inject a minimal header for precinct info extraction
            pct_name_for_header = precinct_pages[pct_id][0]
            pct_header_match = re.search(
                r"(M-?\s*#\s*OF\s*Machine Ballots.*?REGISTERED VOTERS:\s*[\d,]+\s*[\d.]*%?)",
                pct_name_for_header, re.DOTALL
            )
            pct_line_match = re.search(r"(\d{2}-.+?)(?:\n|$)", pct_name_for_header)
            if pct_header_match and pct_line_match:
                combined_section = pct_line_match.group(1) + "\n" + pct_header_match.group(1) + "\n" + combined_section

            pct_info = extract_precinct_info(combined_section, fmt)
            if not pct_info:
                continue

            precincts.append(pct_info)
            results = parse_race_results(combined_section, pct_info, fmt)
            all_results.extend(results)

    else:
        # Formats A, B, C1: split by precinct header
        if fmt in ("A", "B"):
            sections = re.split(r"(?=E - # Of Election Day\s+\d+\s+PRECINCT STATUS:)", full_text)
        elif fmt == "C1":
            sections = re.split(r"(?=M - # Of Machine Ballots\s+\d+\s+PRECINCT STATUS:)", full_text)
        else:
            sections = re.split(r"(?=M-?\s*#\s*OF\s*Machine Ballots?\s+\d+)", full_text)

        for section in sections:
            if len(section.strip()) < 50:
                continue

            pct_info = extract_precinct_info(section, fmt)
            if not pct_info:
                continue

            precincts.append(pct_info)
            results = parse_race_results(section, pct_info, fmt)
            all_results.extend(results)

    print(f"  Parsed {len(precincts)} precincts, {len(all_results)} result rows")

    return {
        "election_date": election_date,
        "election_name": election_name,
        "election_type": election_type,
        "precincts": precincts,
        "results": all_results,
        "source_file": os.path.basename(filepath),
        "has_precinct_data": True,
    }


def extract_precinct_info(section, fmt):
    """Extract precinct metadata from a section."""

    pct_id = None
    pct_name = None

    if fmt in ("A", "C1"):
        # Separate Precinct ID and Precinct Name lines
        id_match = re.search(r"Precinct ID:\s*(\d+)", section)
        name_match = re.search(r"Precinct Name:\s*(.+?)(?:\n|$)", section)
        if id_match:
            pct_id = id_match.group(1)
        if name_match:
            pct_name = name_match.group(1).strip()
    else:
        # Formats B and C2: combined "01-Center 01" or "01-Center 1" line
        # Look for the pattern after header lines but before the VOTES section
        match = re.search(r"(?:^|\n)(\d{2})-(.+?)(?:\n|$)", section)
        if match:
            pct_id = match.group(1)
            pct_name = match.group(2).strip()

    if not pct_id and not pct_name:
        return None

    if not pct_name:
        pct_name = f"Precinct {pct_id}"

    # Extract registration and turnout
    reg_match = re.search(r"REGISTERED VOTERS:\s*([\d,]+)", section)
    count_match = re.search(r"PUBLIC COUNT:\s*([\d,]+)", section)

    # Turnout may be "VOTER TURNOUT: 67.72%" or "REGISTERED VOTERS: 788 69.54%"
    turnout_match = re.search(r"VOTER TURNOUT:\s*([\d.]+)%", section)
    if not turnout_match:
        # Older format: turnout percentage right after registered voters count
        turnout_match2 = re.search(r"REGISTERED VOTERS:\s*[\d,]+\s+([\d.]+)%", section)
        if turnout_match2:
            turnout_pct = float(turnout_match2.group(1))
        else:
            turnout_pct = None
    else:
        turnout_pct = float(turnout_match.group(1))

    return {
        "precinct_id": pct_id or "00",
        "precinct_name": pct_name,
        "registered_voters": int(reg_match.group(1).replace(",", "")) if reg_match else None,
        "turnout_pct": turnout_pct,
        "public_count": int(count_match.group(1).replace(",", "")) if count_match else None,
    }


def extract_race_party(race_name):
    """Extract party prefix from primary race names like '(R) Governor'.
    Returns (cleaned_race_name, party_code) tuple.
    If no party prefix, returns (race_name, None).
    """
    party_map = {"R": "R", "D": "D", "L": "L", "REP": "R", "DEM": "D", "LIB": "L"}
    match = re.match(r"^\((\w+)\)\s+(.+)", race_name)
    if match:
        code = match.group(1).upper()
        if code in party_map:
            return match.group(2).strip(), party_map[code]
    return race_name, None


def parse_race_results(section, pct_info, fmt):
    """Parse all race results from a precinct section."""
    results = []

    # Two race block patterns exist across years:
    #
    # NEWER (2016+, format A/B/C1):
    #   VOTE FOR 1
    #   Race Name
    #   VOTES=N
    #   <candidate lines>
    #
    # OLDER (2010-2014, format C2):
    #   VOTES= N Race Name
    #   VOTE FOR 1
    #   <candidate lines>

    # Candidate line pattern (same across all formats)
    cand_pattern = re.compile(
        r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)%\s+(.+)"
    )

    # Try NEWER pattern first: VOTE FOR N\n[Race]\nVOTES=N\n<candidates>
    race_blocks_new = list(re.finditer(
        r"VOTE FOR (\d+)\n(?:VOTES=([\d,]+)\s+)?(.+?)(?:\nVOTES=([\d,]+))?\n((?:.*?\n)*?)(?=VOTE FOR \d+|Straight Party|Precinct Summary Report|$)",
        section
    ))

    # Try OLDER pattern: VOTES= N Race Name\nVOTE FOR N\n<candidates>
    # Terminator must match (R)/(D) prefixed race names in primaries
    race_blocks_old = list(re.finditer(
        r"VOTES=\s*([\d,]+)\s+(.+?)\nVOTE FOR (\d+)\n((?:.*?\n)*?)(?=VOTES=\s*[\d,]+\s+\S|Straight Party|$)",
        section
    ))

    # Prefer the pattern that finds more blocks. For C2 format, prefer old pattern
    # when counts are equal, since C2 uses VOTES= N Race\nVOTE FOR N ordering.
    use_new = (len(race_blocks_new) > len(race_blocks_old)) if fmt == "C2" else (len(race_blocks_new) >= len(race_blocks_old))
    if use_new:
        # Use newer pattern
        for match in race_blocks_new:
            vote_for = int(match.group(1))
            total_votes_str = match.group(2) or match.group(4)
            race_name = match.group(3).strip()
            race_name = re.sub(r"^VOTES[\s=]*[\d,]+\s*", "", race_name).strip()
            candidates_block = match.group(5)

            # Extract party from race name prefix (e.g., "(R) Governor" in primaries)
            clean_race_name, race_party = extract_race_party(race_name)

            for cand_match in cand_pattern.finditer(candidates_block):
                v1, v2, v3, total_votes = int(cand_match.group(1)), int(cand_match.group(2)), int(cand_match.group(3)), int(cand_match.group(4))
                pct = float(cand_match.group(5))
                name_str = cand_match.group(6).strip()
                party, candidate_name = parse_candidate_name(name_str)
                # If candidate has no party but race has a party prefix, use race party
                if party is None and race_party is not None:
                    party = race_party
                results.append({
                    "precinct_id": pct_info["precinct_id"],
                    "precinct_name": pct_info["precinct_name"],
                    "race_name": clean_race_name,
                    "vote_for": vote_for,
                    "candidate_name": candidate_name,
                    "party": party,
                    "v1": v1, "v2": v2, "v3": v3,
                    "total_votes": total_votes,
                    "vote_percentage": pct,
                })
    else:
        # Use older pattern
        for match in race_blocks_old:
            total_votes_str = match.group(1)
            race_name = match.group(2).strip()
            vote_for = int(match.group(3))
            candidates_block = match.group(4)

            # Extract party from race name prefix (e.g., "(R) Governor" in primaries)
            clean_race_name, race_party = extract_race_party(race_name)

            for cand_match in cand_pattern.finditer(candidates_block):
                v1, v2, v3, total_votes = int(cand_match.group(1)), int(cand_match.group(2)), int(cand_match.group(3)), int(cand_match.group(4))
                pct = float(cand_match.group(5))
                name_str = cand_match.group(6).strip()
                party, candidate_name = parse_candidate_name(name_str)
                # If candidate has no party but race has a party prefix, use race party
                if party is None and race_party is not None:
                    party = race_party
                results.append({
                    "precinct_id": pct_info["precinct_id"],
                    "precinct_name": pct_info["precinct_name"],
                    "race_name": clean_race_name,
                    "vote_for": vote_for,
                    "candidate_name": candidate_name,
                    "party": party,
                    "v1": v1, "v2": v2, "v3": v3,
                    "total_votes": total_votes,
                    "vote_percentage": pct,
                })

    # Parse straight party votes
    straight_matches = re.findall(
        r"(Democratic Party|Republican Party|Libertarian Party)\s+(\d+)",
        section
    )
    for party_name, votes in straight_matches:
        party_code = {
            "Democratic Party": "D",
            "Republican Party": "R",
            "Libertarian Party": "L"
        }.get(party_name, party_name)
        results.append({
            "precinct_id": pct_info["precinct_id"],
            "precinct_name": pct_info["precinct_name"],
            "race_name": "Straight Party",
            "vote_for": 1,
            "candidate_name": party_name,
            "party": party_code,
            "v1": None, "v2": None, "v3": None,
            "total_votes": int(votes),
            "vote_percentage": None,
        })

    return results


def parse_summary_report(full_text, election_date, election_name, election_type, filepath):
    """Parse a summary-only report (county-wide totals, no precinct breakdown)."""
    results = []

    race_blocks = re.finditer(
        r"VOTE(?:S)?\s+FOR\s+(\d+)\n(?:VOTES\s*=\s*([\d,]+)\s+)?(.+?)(?:\nVOTES\s*=\s*([\d,]+))?\n((?:.*?\n)*?)(?=VOTE(?:S)?\s+FOR\s+\d+|Straight Party|$)",
        full_text
    )

    for match in race_blocks:
        vote_for = int(match.group(1))
        total_votes_str = match.group(2) or match.group(4)
        race_name = match.group(3).strip()
        race_name = re.sub(r"^VOTES\s*=\s*[\d,]+\s*", "", race_name).strip()
        candidates_block = match.group(5)

        # Summary reports may have 3 or 4 column formats
        # Try 3-column first (M A P TOTAL) then 2-column (just TOTAL %)
        cand_pattern = re.compile(
            r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)%\s+(.+)"
        )

        for cand_match in cand_pattern.finditer(candidates_block):
            total_votes = int(cand_match.group(4))
            pct = float(cand_match.group(5))
            name_str = cand_match.group(6).strip()
            party, candidate_name = parse_candidate_name(name_str)

            results.append({
                "precinct_id": "ALL",
                "precinct_name": "County Total",
                "race_name": race_name,
                "vote_for": vote_for,
                "candidate_name": candidate_name,
                "party": party,
                "v1": int(cand_match.group(1)),
                "v2": int(cand_match.group(2)),
                "v3": int(cand_match.group(3)),
                "total_votes": total_votes,
                "vote_percentage": pct,
            })

    print(f"  Parsed county-wide summary: {len(results)} result rows")

    return {
        "election_date": election_date,
        "election_name": election_name,
        "election_type": election_type,
        "precincts": [],
        "results": results,
        "source_file": os.path.basename(filepath),
        "has_precinct_data": False,
    }


def parse_candidate_name(name_str):
    """Parse party affiliation and candidate name from a result line."""
    party = None
    candidate_name = name_str

    # Pattern: "(R) Name" or "(D) Name" or "(REP) Name" etc
    party_match = re.match(r"\((\w+)\)\s+(.+)", name_str)
    if party_match:
        party_code = party_match.group(1).upper()
        candidate_name = party_match.group(2).strip()
        party_map = {
            "R": "R", "REP": "R",
            "D": "D", "DEM": "D",
            "L": "L", "LIB": "L",
            "WTP": "WTP",
            "NP": "NP",
        }
        party = party_map.get(party_code, party_code)
    # Pattern: "Name (R)" at end
    elif re.search(r"\(([RDL])\)\s*$", name_str):
        end_match = re.search(r"(.+?)\s*\(([RDL])\)\s*$", name_str)
        if end_match:
            candidate_name = end_match.group(1).strip()
            party = end_match.group(2)
    # 2012 style: "Romney-Ryan (R)"
    elif re.search(r"\((R|D|L)\)\s*$", name_str):
        end_match = re.search(r"(.+?)\s*\((R|D|L)\)\s*$", name_str)
        if end_match:
            candidate_name = end_match.group(1).strip()
            party = end_match.group(2)
    elif name_str in ("Yes", "YES", "No", "NO", "Write-In", "WRITE-IN"):
        candidate_name = name_str.title()
        party = None

    return party, candidate_name


def classify_race_level(race_name):
    """Classify a race into federal/state/county/etc level."""
    name_lower = race_name.lower()
    if any(kw in name_lower for kw in ["president", "united states senator", "united states rep", "us rep", "us senator"]):
        return "federal"
    if any(kw in name_lower for kw in ["governor", "attorney general", "secretary of state",
                                        "auditor of state", "treasurer of state",
                                        "state senator", "state rep", "supreme court", "court of appeals"]):
        return "state"
    if any(kw in name_lower for kw in ["county", "circuit court", "coroner", "commissioner", "council",
                                        "auditor", "recorder", "treasurer", "sheriff", "surveyor",
                                        "assessor", "prosecuting", "circuit court clerk"]):
        return "county"
    if any(kw in name_lower for kw in ["school", "community school", "twp", "township", "town council"]):
        return "local"
    if any(kw in name_lower for kw in ["public question", "const amendment", "straight party"]):
        return "ballot_measure"
    return "other"


def load_parsed_into_db(parsed_data, db_path=None):
    """Load parsed data into the BCD database."""
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
        "SELECT id FROM elections WHERE county_id = ? AND election_date = ? AND election_type = ?",
        (county_id, election_date, election_type)
    )
    existing = cursor.fetchone()
    if existing:
        print(f"  Election already in database (ID {existing['id']}), skipping.")
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
        "SELECT id FROM elections WHERE county_id = ? AND election_date = ? AND election_type = ?",
        (county_id, election_date, election_type)
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

    # Create races, candidates, results
    race_cache = {}
    candidate_cache = {}
    records = 0

    for row in parsed_data["results"]:
        race_name = row["race_name"]
        if race_name not in race_cache:
            race_level = classify_race_level(race_name)
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
           VALUES (?, 'pdf', ?, 'success')""",
        (parsed_data["source_file"], records)
    )

    conn.commit()
    conn.close()

    print(f"  Loaded: {records} records, {len(race_cache)} races, {len(candidate_cache)} candidates")
    return records


def process_all_pdfs(pdf_dir="data/raw_pdfs", db_path=None):
    """Process all PDFs in the directory and load into database."""
    files = sorted([f for f in os.listdir(pdf_dir) if f.endswith(".pdf")])

    print(f"Found {len(files)} PDF files")
    print("=" * 70)

    results_summary = []

    for f in files:
        filepath = os.path.join(pdf_dir, f)
        print(f"\n{'='*70}")
        print(f"Processing: {f}")

        try:
            parsed = parse_pdf_universal(filepath)
            if parsed:
                records = load_parsed_into_db(parsed, db_path)
                results_summary.append({
                    "file": f,
                    "election": parsed["election_name"],
                    "date": parsed["election_date"],
                    "precincts": len(parsed["precincts"]),
                    "results": len(parsed["results"]),
                    "loaded": records,
                    "status": "OK" if records > 0 else "SKIPPED (already loaded)",
                })
            else:
                results_summary.append({
                    "file": f,
                    "election": "?",
                    "date": "?",
                    "precincts": 0,
                    "results": 0,
                    "loaded": 0,
                    "status": "FAILED (no text - scanned PDF)",
                })
        except Exception as e:
            print(f"  ERROR: {e}")
            results_summary.append({
                "file": f,
                "election": "?",
                "date": "?",
                "precincts": 0,
                "results": 0,
                "loaded": 0,
                "status": f"ERROR: {str(e)[:50]}",
            })

    # Print summary
    print(f"\n\n{'='*70}")
    print("PROCESSING SUMMARY")
    print(f"{'='*70}")
    df = pd.DataFrame(results_summary)
    print(df.to_string(index=False))

    return results_summary


if __name__ == "__main__":
    if "--all" in sys.argv:
        process_all_pdfs()
    elif len(sys.argv) > 1:
        filepath = sys.argv[1]
        parsed = parse_pdf_universal(filepath)
        if parsed:
            if "--load" in sys.argv:
                load_parsed_into_db(parsed)
            else:
                print(f"\nResults preview ({len(parsed['results'])} rows):")
                df = pd.DataFrame(parsed["results"])
                print(df.head(20).to_string(index=False))
    else:
        print("Usage:")
        print("  python parse_all_pdfs.py --all              Process all PDFs in data/raw_pdfs/")
        print("  python parse_all_pdfs.py <file>             Preview a single PDF")
        print("  python parse_all_pdfs.py <file> --load      Parse and load a single PDF")
