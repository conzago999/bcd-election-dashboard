"""
BCD Data Validation & Repair Script
Validates DB data against source PDFs and fixes:
  1. Reclassifies legitimate 'other' races to correct race_level
  2. Re-parses corrupted races (vote data in race_name) from source PDFs
  3. Normalizes race names for consistency across elections
  4. Cross-validates PDF totals vs DB totals
"""

import sqlite3
import re
import os
import sys
import pdfplumber
from collections import defaultdict

# Add src to path
sys.path.insert(0, os.path.dirname(__file__))
from database import get_connection, DB_PATH

# ============================================================
# PHASE 1: Reclassify legitimate 'other' races
# ============================================================

def fix_race_level_classification(db_path=None):
    """
    Fix race_level for races that classify_race_level() missed.
    Returns count of updated rows.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()

    # Extended classification rules (what the original missed)
    rules = [
        # Federal
        ("federal", "race_name LIKE '%U.S. Representative%'"),
        ("federal", "race_name LIKE '%US Representative%'"),
        ("federal", "race_name LIKE '%U.S. Rep%' AND race_name NOT LIKE '%State Rep%'"),
        ("federal", "race_name LIKE '%Congress%'"),

        # State
        ("state", "race_name LIKE '%Lieutenant Governor%'"),
        ("state", "race_name LIKE '%Superintendent%Public Instruction%'"),

        # Local
        ("local", "race_name LIKE '%Mayor%'"),
        ("local", "race_name LIKE '%Town Council%'"),
        ("local", "race_name LIKE '%Town Board%'"),
        ("local", "race_name LIKE '%Trustee%'"),
        ("local", "race_name LIKE '%School Board%'"),
        ("local", "race_name LIKE '%Community School%'"),
        ("local", "race_name LIKE '%Township%'"),
        ("local", "race_name LIKE '%Twp %'"),
        ("local", "race_name LIKE '%Zionsville%' AND race_name NOT LIKE '%Pct Committeeman%'"),
        ("local", "race_name LIKE '%Lebanon%' AND race_name NOT LIKE '%Pct Committeeman%'"),
        ("local", "race_name LIKE '%Whitestown%' AND race_name NOT LIKE '%Pct Committeeman%'"),

        # Party-internal (classify as 'party' — skip in analytics)
        ("party", "race_name LIKE '%Committeeman%'"),
        ("party", "race_name LIKE '%Delegate%'"),
        ("party", "race_name LIKE '%Convention%'"),
        ("party", "race_name LIKE '%Precinct Committeeperson%'"),
    ]

    total_updated = 0
    for level, condition in rules:
        sql = f"""
            UPDATE races SET race_level = '{level}'
            WHERE race_level = 'other'
            AND NOT (race_name GLOB '*[0-9] [0-9]*[0-9] [0-9]*')
            AND {condition}
        """
        cur.execute(sql)
        count = cur.rowcount
        if count > 0:
            print(f"  Reclassified {count} races to '{level}' via: {condition}")
            total_updated += count

    conn.commit()
    conn.close()
    return total_updated


# ============================================================
# PHASE 2: Identify and handle corrupted races
# ============================================================

def identify_corrupted_races(db_path=None):
    """
    Find races where race_name contains vote data (parsing failure).
    Pattern: "NNN NN N NNN NN.NN% CANDIDATE_NAME"
    Returns list of (race_id, race_name, election_date, election_id).
    """
    conn = get_connection(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT r.id, r.race_name, e.election_date, e.id as election_id, e.election_type
        FROM races r
        JOIN elections e ON r.election_id = e.id
        WHERE r.race_level = 'other'
        AND r.race_name GLOB '*[0-9] [0-9]*[0-9]%*'
        ORDER BY e.election_date, r.id
    """)
    corrupted = cur.fetchall()
    conn.close()
    return corrupted


def parse_corrupted_race_name(race_name):
    """
    Extract embedded vote data from a corrupted race_name.
    E.g., "244 32 0 276 68.15% DAN COATS (R)" ->
    {'v1': 244, 'v2': 32, 'v3': 0, 'total': 276, 'pct': 68.15,
     'candidate': 'DAN COATS', 'party': 'R'}
    """
    match = re.match(
        r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)%\s+(.+)",
        race_name
    )
    if not match:
        return None

    name_str = match.group(6).strip()

    # Extract party from end: "NAME (R)" or "(R) NAME"
    party = None
    candidate = name_str

    party_end = re.search(r"(.+?)\s*\(([RDL])\)\s*$", name_str)
    party_start = re.match(r"\(([RDL])\)\s+(.+)", name_str)

    if party_end:
        candidate = party_end.group(1).strip()
        party = party_end.group(2)
    elif party_start:
        party = party_start.group(1)
        candidate = party_start.group(2).strip()

    return {
        "v1": int(match.group(1)),
        "v2": int(match.group(2)),
        "v3": int(match.group(3)),
        "total": int(match.group(4)),
        "pct": float(match.group(5)),
        "candidate": candidate,
        "party": party,
    }


# ============================================================
# PHASE 3: Re-parse PDFs to get ground truth race names
# ============================================================

def extract_races_from_pdf(pdf_path):
    """
    Extract race structure from a PDF without full parsing.
    Returns dict: {precinct_name: [(race_name, [(candidate, party, votes), ...]), ...]}
    """
    all_text = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                all_text.append(text)

    if not all_text:
        return None

    full_text = "\n".join(all_text)

    # Extract all race names in order
    races = []

    # Pattern for newer format: VOTE FOR N\nRace Name
    for m in re.finditer(r"VOTE FOR (\d+)\n(.+?)(?:\n|$)", full_text):
        race_name = m.group(2).strip()
        # Skip if it looks like a VOTES= line
        if race_name.startswith("VOTES"):
            continue
        # Skip lines that look like candidate data
        if re.match(r"\d+\s+\d+\s+\d+\s+\d+", race_name):
            continue
        races.append(race_name)

    # Pattern for older format: VOTES= N Race Name
    for m in re.finditer(r"VOTES=\s*[\d,]+\s+(.+?)(?:\n|$)", full_text):
        race_name = m.group(1).strip()
        if not re.match(r"\d+\s+\d+\s+\d+\s+\d+", race_name):
            races.append(race_name)

    # Deduplicate while preserving order
    seen = set()
    unique_races = []
    for r in races:
        if r not in seen:
            seen.add(r)
            unique_races.append(r)

    return unique_races


def build_pdf_race_inventory(pdf_dir="/Users/jb/Downloads"):
    """
    Build mapping: election_date -> [race_names from PDF]
    Uses filename patterns to match elections.
    """
    # Map PDF filenames to election dates
    pdf_date_map = {}

    for f in os.listdir(pdf_dir):
        if not f.endswith(".pdf"):
            continue
        filepath = os.path.join(pdf_dir, f)
        f_lower = f.lower()

        # Try to extract year and type from filename
        year_match = re.search(r"(20\d{2})", f)
        if not year_match:
            continue
        year = year_match.group(1)

        if "primary" in f_lower or "primary" in f_lower:
            key = f"{year}-primary"
        elif "general" in f_lower:
            key = f"{year}-general"
        elif "special" in f_lower:
            key = f"{year}-special"
        elif "precbyprecreport" in f_lower or "precinctbyprecinct" in f_lower:
            key = f"{year}-primary"  # These tend to be precinct reports
        else:
            continue

        if key not in pdf_date_map:
            pdf_date_map[key] = filepath

    return pdf_date_map


# ============================================================
# PHASE 4: Cross-validate DB vs PDF
# ============================================================

def cross_validate_election(election_date, pdf_path, db_path=None):
    """
    Compare DB data for one election against the source PDF.
    Returns validation report dict.
    """
    report = {
        "election_date": election_date,
        "pdf_file": os.path.basename(pdf_path),
        "issues": [],
        "stats": {}
    }

    # Get DB data
    conn = get_connection(db_path)
    cur = conn.cursor()

    # DB race count
    cur.execute("""
        SELECT COUNT(DISTINCT r.id) as race_count,
               COUNT(res.id) as result_count,
               SUM(res.votes) as total_votes
        FROM races r
        JOIN elections e ON r.election_id = e.id
        LEFT JOIN results res ON res.race_id = r.id
        WHERE e.election_date = ?
    """, (election_date,))
    db_stats = cur.fetchone()
    report["stats"]["db_races"] = db_stats["race_count"]
    report["stats"]["db_results"] = db_stats["result_count"]
    report["stats"]["db_total_votes"] = db_stats["total_votes"]

    # DB corrupted count
    cur.execute("""
        SELECT COUNT(*) FROM races r
        JOIN elections e ON r.election_id = e.id
        WHERE e.election_date = ? AND r.race_level = 'other'
        AND r.race_name GLOB '*[0-9] [0-9]*[0-9]%*'
    """, (election_date,))
    report["stats"]["corrupted_races"] = cur.fetchone()[0]

    # DB turnout
    cur.execute("""
        SELECT
            COUNT(DISTINCT t.precinct_id) as precinct_count,
            SUM(t.registered_voters) as total_registered,
            SUM(t.ballots_cast) as total_ballots
        FROM turnout t
        JOIN elections e ON t.election_id = e.id
        WHERE e.election_date = ?
    """, (election_date,))
    turnout = cur.fetchone()
    report["stats"]["db_precincts"] = turnout["precinct_count"]
    report["stats"]["db_registered"] = turnout["total_registered"]
    report["stats"]["db_ballots"] = turnout["total_ballots"]

    conn.close()

    # Parse PDF for comparison
    try:
        with pdfplumber.open(pdf_path) as pdf:
            first_page = pdf.pages[0].extract_text() or ""

            # Extract PDF-level totals from first page
            reg_match = re.search(r"REGISTERED VOTERS:\s*([\d,]+)", first_page)
            count_match = re.search(r"PUBLIC COUNT:\s*([\d,]+)", first_page)
            pct_match = re.search(r"NUMBER OF PRECINCTS:\s*(\d+)", first_page)

            if reg_match:
                pdf_registered = int(reg_match.group(1).replace(",", ""))
                report["stats"]["pdf_registered"] = pdf_registered
                if report["stats"]["db_registered"] and abs(pdf_registered - report["stats"]["db_registered"]) > 10:
                    report["issues"].append(
                        f"Registered voters mismatch: PDF={pdf_registered}, DB={report['stats']['db_registered']}"
                    )

            if count_match:
                pdf_ballots = int(count_match.group(1).replace(",", ""))
                report["stats"]["pdf_ballots"] = pdf_ballots
                if report["stats"]["db_ballots"] and abs(pdf_ballots - report["stats"]["db_ballots"]) > 10:
                    report["issues"].append(
                        f"Ballots cast mismatch: PDF={pdf_ballots}, DB={report['stats']['db_ballots']}"
                    )

            if pct_match:
                pdf_precincts = int(pct_match.group(1))
                report["stats"]["pdf_precincts"] = pdf_precincts
                if pdf_precincts != report["stats"]["db_precincts"]:
                    report["issues"].append(
                        f"Precinct count mismatch: PDF={pdf_precincts}, DB={report['stats']['db_precincts']}"
                    )

        # Extract race names from PDF
        pdf_races = extract_races_from_pdf(pdf_path)
        if pdf_races:
            report["stats"]["pdf_race_names"] = len(pdf_races)
            report["pdf_races"] = pdf_races
    except Exception as e:
        report["issues"].append(f"PDF read error: {e}")

    if report["stats"]["corrupted_races"] > 0:
        report["issues"].append(
            f"{report['stats']['corrupted_races']} races have vote data in race_name (parsing failure)"
        )

    return report


# ============================================================
# PHASE 5: Normalize race names
# ============================================================

RACE_NAME_NORMALIZATIONS = {
    # Federal
    "President of the United States": "President of the United States",
    "President": "President of the United States",
    "U.S. Representative": None,  # Keep district number
    "US Representative": None,    # Normalize prefix
    "United States Senator": "United States Senator",
    "U.S. Senator": "United States Senator",

    # State
    "State Rep ": "State Representative ",
    "State Rep. ": "State Representative ",
    "State Senator ": "State Senator ",

    # County
    "Circuit Court Clerk": "Circuit Court Clerk",
    "Prosecuting Attorney": "Prosecuting Attorney",

    # Local - normalize mayor names
    "Mayor Lebanon": "Mayor of Lebanon",
    "Mayor of Lebanon": "Mayor of Lebanon",
    "Mayor Zionsville": "Mayor of Zionsville",
    "Mayor of Zionsville": "Mayor of Zionsville",
    "Zionsville Mayor": "Mayor of Zionsville",
}

def normalize_race_names(db_path=None, dry_run=True):
    """
    Normalize race name variants across elections.
    If dry_run=True, just reports what would change.
    Returns list of (old_name, new_name, count) tuples.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()

    changes = []

    # Rule-based normalizations
    normalization_rules = [
        # "US Representative" -> "U.S. Representative"
        ("race_name LIKE 'US Representative%'",
         "REPLACE(race_name, 'US Representative', 'U.S. Representative')"),

        # "State Rep District" -> "State Representative District"
        ("race_name LIKE 'State Rep District%' OR race_name LIKE '(%) State Rep District%'",
         "REPLACE(race_name, 'State Rep District', 'State Representative District')"),

        # Mayor normalizations
        ("race_name = 'Mayor Lebanon' OR race_name = 'Mayor of Lebanon'",
         "'Mayor of Lebanon'"),
        ("race_name = 'Zionsville Mayor' OR race_name = 'Mayor Zionsville'",
         "'Mayor of Zionsville'"),
        ("race_name = 'Mayor of Lebanon'", "'Mayor of Lebanon'"),
        ("race_name = 'Mayor of Zionsville'", "'Mayor of Zionsville'"),

        # Straight Party consistent
        ("race_name = 'STRAIGHT PARTY' OR race_name = 'straight party'",
         "'Straight Party'"),
    ]

    # First, audit what we'd change
    cur.execute("""
        SELECT DISTINCT race_name, COUNT(*) as cnt
        FROM races
        WHERE race_level != 'other' OR NOT (race_name GLOB '*[0-9] [0-9]*[0-9]%*')
        GROUP BY race_name
        ORDER BY race_name
    """)
    all_names = cur.fetchall()

    # Find similar names that should be unified
    name_groups = defaultdict(list)
    for row in all_names:
        # Normalize for comparison: strip party prefix, lowercase
        name = row["race_name"]
        clean = re.sub(r"^\([RDL]\)\s+", "", name).strip().lower()
        clean = re.sub(r"\s+", " ", clean)
        name_groups[clean].append((name, row["cnt"]))

    # Report variants
    for key, variants in sorted(name_groups.items()):
        if len(variants) > 1:
            changes.append({
                "key": key,
                "variants": variants,
                "action": "REVIEW - multiple variants"
            })

    conn.close()
    return changes


# ============================================================
# PHASE 6: Fix the parser's classify_race_level
# ============================================================

def improved_classify_race_level(race_name):
    """
    Improved race classification that catches all known patterns.
    """
    name_lower = race_name.lower()
    # Strip party prefix for matching
    clean = re.sub(r"^\([rdl]\)\s+", "", name_lower)

    # Federal
    if any(kw in clean for kw in [
        "president", "united states senator", "u.s. senator",
        "united states rep", "u.s. rep", "us rep",
        "congress"
    ]):
        return "federal"

    # State
    if any(kw in clean for kw in [
        "governor", "lieutenant governor",
        "attorney general", "secretary of state",
        "auditor of state", "treasurer of state",
        "superintendent of public instruction",
        "state senator", "state rep",
        "supreme court", "court of appeals"
    ]):
        return "state"

    # County
    if any(kw in clean for kw in [
        "county", "circuit court", "coroner", "commissioner",
        "council member", "council at large", "council district",
        "auditor", "recorder", "treasurer", "sheriff",
        "surveyor", "assessor", "prosecuting", "clerk"
    ]):
        # But not "town council"
        if "town council" not in clean:
            return "county"

    # Local
    if any(kw in clean for kw in [
        "school", "community school", "twp", "township",
        "town council", "town board", "mayor",
        "zionsville", "lebanon", "whitestown", "advance",
        "thorntown", "jamestown", "ulen"
    ]):
        return "local"

    # Ballot measures
    if any(kw in clean for kw in [
        "public question", "constitutional amendment",
        "const amendment", "referendum", "straight party"
    ]):
        return "ballot_measure"

    # Party internal
    if any(kw in clean for kw in [
        "committeeman", "committeewoman", "committeeperson",
        "delegate", "convention", "state conv"
    ]):
        return "party"

    return "other"


# ============================================================
# MAIN: Run full validation
# ============================================================

def run_full_validation(db_path=None, pdf_dir="/Users/jb/Downloads"):
    """Run the complete validation suite and print report."""

    if db_path is None:
        db_path = DB_PATH

    print("=" * 70)
    print("BCD DATA VALIDATION & REPAIR REPORT")
    print("=" * 70)

    # --- Phase 1: Reclassify legitimate 'other' races ---
    print("\n--- PHASE 1: Reclassify Legitimate 'Other' Races ---")

    conn = get_connection(db_path)
    cur = conn.cursor()

    # Count before
    cur.execute("SELECT COUNT(*) FROM races WHERE race_level = 'other'")
    before_other = cur.fetchone()[0]

    # Count truly corrupted
    cur.execute("""
        SELECT COUNT(*) FROM races WHERE race_level = 'other'
        AND race_name GLOB '*[0-9] [0-9]*[0-9]%*'
    """)
    corrupted_count = cur.fetchone()[0]

    # Count legitimate 'other'
    legit_other = before_other - corrupted_count

    print(f"  Total race_level='other': {before_other}")
    print(f"  Corrupted (vote data in name): {corrupted_count}")
    print(f"  Legitimate but misclassified: {legit_other}")
    conn.close()

    updated = fix_race_level_classification(db_path)
    print(f"  => Reclassified {updated} races")

    # Show remaining 'other' that aren't corrupted
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT race_name FROM races
        WHERE race_level = 'other'
        AND NOT (race_name GLOB '*[0-9] [0-9]*[0-9]%*')
        ORDER BY race_name
    """)
    remaining = cur.fetchall()
    if remaining:
        print(f"\n  Remaining unclassified 'other' ({len(remaining)}):")
        for row in remaining:
            print(f"    - {row[0]}")
    conn.close()

    # --- Phase 2: Analyze corrupted races ---
    print(f"\n--- PHASE 2: Corrupted Race Analysis ---")
    corrupted = identify_corrupted_races(db_path)

    elections_affected = defaultdict(int)
    for race in corrupted:
        elections_affected[race["election_date"]] += 1

    print(f"  Total corrupted races: {len(corrupted)}")
    print(f"  Elections affected:")
    for date, count in sorted(elections_affected.items()):
        print(f"    {date}: {count} corrupted race entries")

    # Try to decode a few
    print(f"\n  Sample decoded corrupted entries:")
    for race in corrupted[:5]:
        decoded = parse_corrupted_race_name(race["race_name"])
        if decoded:
            print(f"    Race ID {race['id']}: {decoded['candidate']} ({decoded['party']}) = {decoded['total']} votes ({decoded['pct']}%)")

    # --- Phase 3: Cross-validate with PDFs ---
    print(f"\n--- PHASE 3: PDF Cross-Validation ---")

    # Map election dates to source PDFs
    pdf_map = {
        "2024-05-07": f"{pdf_dir}/2024precbyprecreport.pdf",
        "2024-11-05": f"{pdf_dir}/allprecinctsafterprov.pdf",
        "2022-11-08": f"{pdf_dir}/precinctbyprecinctafterP.pdf",
        "2020-11-03": f"{pdf_dir}/2020-General-Election-Results.pdf",
        "2019-11-05": f"{pdf_dir}/2019-General-Election-Results.pdf",
        "2019-05-07": f"{pdf_dir}/2019-Primary-Election-Results.pdf",
        "2018-11-06": f"{pdf_dir}/2018-General-Election-Results.pdf",
        "2018-05-08": f"{pdf_dir}/2018-Primary-Eleciton-Results.pdf",
        "2016-11-08": f"{pdf_dir}/2016-General-Election-Results.pdf",
        "2016-05-03": f"{pdf_dir}/2016-Primary-Election-Results.pdf",
        "2014-11-04": f"{pdf_dir}/2014-General-Election-Results.pdf",
        "2014-05-06": f"{pdf_dir}/2014-Primary-Election-Results.pdf",
        "2012-11-06": f"{pdf_dir}/2012-General-Election-Results.pdf",
        "2012-05-08": f"{pdf_dir}/2012-Primary-Election-Results.pdf",
        "2010-11-02": f"{pdf_dir}/2010-General-Election-Results.pdf",
        "2010-05-04": f"{pdf_dir}/2010-Primary-Election-Results.pdf",
    }

    for date, pdf_path in sorted(pdf_map.items()):
        if not os.path.exists(pdf_path):
            print(f"  {date}: PDF not found at {pdf_path}")
            continue

        report = cross_validate_election(date, pdf_path, db_path)
        status = "OK" if not report["issues"] else "ISSUES"
        print(f"\n  {date} [{status}] ({report['pdf_file']})")
        print(f"    DB: {report['stats']['db_races']} races, {report['stats']['db_results']} results, {report['stats']['db_precincts']} precincts")
        if "pdf_registered" in report["stats"]:
            print(f"    PDF: registered={report['stats'].get('pdf_registered', '?')}, ballots={report['stats'].get('pdf_ballots', '?')}, precincts={report['stats'].get('pdf_precincts', '?')}")
        for issue in report["issues"]:
            print(f"    !! {issue}")

    # --- Phase 4: Race name analysis ---
    print(f"\n--- PHASE 4: Race Name Normalization Analysis ---")
    changes = normalize_race_names(db_path, dry_run=True)

    variant_count = 0
    for change in changes:
        if len(change["variants"]) > 1:
            variant_count += 1
            if variant_count <= 15:  # Show first 15
                variants_str = ", ".join(f"'{v[0]}' (x{v[1]})" for v in change["variants"])
                print(f"  {change['key']}: {variants_str}")

    if variant_count > 15:
        print(f"  ... and {variant_count - 15} more variant groups")
    print(f"\n  Total race name variant groups: {variant_count}")

    # --- Summary ---
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute("SELECT race_level, COUNT(*) FROM races GROUP BY race_level ORDER BY COUNT(*) DESC")
    print("\n  Race level distribution (after Phase 1 fix):")
    for row in cur.fetchall():
        print(f"    {row[0]}: {row[1]}")

    cur.execute("SELECT COUNT(*) FROM races WHERE race_level = 'other' AND race_name GLOB '*[0-9] [0-9]*[0-9]%*'")
    remaining_corrupted = cur.fetchone()[0]
    print(f"\n  Remaining corrupted races (need re-parse): {remaining_corrupted}")

    cur.execute("SELECT COUNT(*) FROM races WHERE race_level = 'other' AND NOT (race_name GLOB '*[0-9] [0-9]*[0-9]%*')")
    remaining_other = cur.fetchone()[0]
    print(f"  Remaining unclassified 'other': {remaining_other}")
    conn.close()

    print("\n  RECOMMENDED ACTIONS:")
    if remaining_corrupted > 0:
        print(f"    1. Re-parse {remaining_corrupted} corrupted races from source PDFs (2010-2014)")
    if remaining_other > 0:
        print(f"    2. Review {remaining_other} remaining 'other' races")
    print(f"    3. Apply race name normalizations ({variant_count} variant groups)")
    print(f"    4. Update classify_race_level() in parse_all_pdfs.py for future imports")


if __name__ == "__main__":
    run_full_validation()
