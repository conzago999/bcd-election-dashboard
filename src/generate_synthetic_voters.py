"""
Synthetic Voter File Generator for BCD Election Dashboard

Generates a realistic fake voter file matching VAN/Indiana voter file schema,
calibrated to real Boone County precinct data, demographics, and vote patterns.

Usage:
    python src/generate_synthetic_voters.py                      # Full 62K voters
    python src/generate_synthetic_voters.py --count 5000         # Smaller test set
    python src/generate_synthetic_voters.py --seed 99            # Different random seed
    python src/generate_synthetic_voters.py --output /tmp/test.csv

The output CSV matches standard VAN export format so it can be swapped
with a real voter file when available.
"""

import os
import sys
import csv
import random
import argparse
import math
from datetime import date, datetime, timedelta

from faker import Faker


# =============================================================================
# REAL BOONE COUNTY DATA (from bcd_elections.db, 2024 General Election)
# =============================================================================

PRECINCT_DATA = {
    "Center 01": {"registered": 731, "turnout": 67.7, "d_share": 14.9, "area": "Lebanon", "township": "Center"},
    "Center 02": {"registered": 980, "turnout": 53.7, "d_share": 14.4, "area": "Lebanon", "township": "Center"},
    "Center 03": {"registered": 1107, "turnout": 44.9, "d_share": 13.5, "area": "Lebanon", "township": "Center"},
    "Center 04": {"registered": 770, "turnout": 51.7, "d_share": 14.8, "area": "Lebanon", "township": "Center"},
    "Center 05": {"registered": 1075, "turnout": 73.6, "d_share": 10.5, "area": "Lebanon", "township": "Center"},
    "Center 06": {"registered": 1096, "turnout": 55.2, "d_share": 14.6, "area": "Lebanon", "township": "Center"},
    "Center 07": {"registered": 1049, "turnout": 60.1, "d_share": 15.5, "area": "Lebanon", "township": "Center"},
    "Center 08": {"registered": 752, "turnout": 40.0, "d_share": 15.1, "area": "Lebanon", "township": "Center"},
    "Center 09": {"registered": 994, "turnout": 63.5, "d_share": 14.7, "area": "Lebanon", "township": "Center"},
    "Center 10": {"registered": 1010, "turnout": 51.8, "d_share": 17.4, "area": "Lebanon", "township": "Center"},
    "Center 11": {"registered": 1065, "turnout": 52.7, "d_share": 13.4, "area": "Lebanon", "township": "Center"},
    "Center 12": {"registered": 1071, "turnout": 71.3, "d_share": 13.3, "area": "Lebanon", "township": "Center"},
    "Center 13": {"registered": 976, "turnout": 47.2, "d_share": 21.1, "area": "Lebanon", "township": "Center"},
    "Center 14": {"registered": 1026, "turnout": 69.1, "d_share": 10.0, "area": "Lebanon", "township": "Center"},
    "Center 15": {"registered": 161, "turnout": 74.5, "d_share": 17.4, "area": "Lebanon", "township": "Center"},
    "Center 16": {"registered": 134, "turnout": 70.9, "d_share": 8.3, "area": "Lebanon", "township": "Center"},
    "Center 17": {"registered": 1170, "turnout": 49.6, "d_share": 12.6, "area": "Lebanon", "township": "Center"},
    "Jefferson": {"registered": 1224, "turnout": 73.6, "d_share": 7.7, "area": "Lebanon", "township": "Jefferson"},
    "Washington": {"registered": 1136, "turnout": 70.7, "d_share": 8.3, "area": "Lebanon", "township": "Washington"},
    "Eagle 01": {"registered": 1264, "turnout": 72.2, "d_share": 24.0, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 02": {"registered": 973, "turnout": 67.9, "d_share": 23.2, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 03": {"registered": 926, "turnout": 71.8, "d_share": 32.6, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 04": {"registered": 1609, "turnout": 56.7, "d_share": 29.1, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 05": {"registered": 977, "turnout": 73.9, "d_share": 24.5, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 06": {"registered": 2440, "turnout": 75.0, "d_share": 21.7, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 07": {"registered": 1500, "turnout": 67.5, "d_share": 21.7, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 08": {"registered": 1367, "turnout": 66.0, "d_share": 23.0, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 09": {"registered": 1854, "turnout": 74.3, "d_share": 23.9, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 10": {"registered": 1963, "turnout": 63.2, "d_share": 23.3, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 11": {"registered": 858, "turnout": 74.1, "d_share": 23.0, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 12": {"registered": 1074, "turnout": 66.3, "d_share": 29.4, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 13": {"registered": 2010, "turnout": 60.8, "d_share": 24.9, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 14": {"registered": 1290, "turnout": 71.8, "d_share": 24.6, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 15": {"registered": 706, "turnout": 71.5, "d_share": 22.8, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 16": {"registered": 1145, "turnout": 77.0, "d_share": 26.5, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 17": {"registered": 1096, "turnout": 60.3, "d_share": 25.5, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 18": {"registered": 1045, "turnout": 69.7, "d_share": 22.3, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 19": {"registered": 1959, "turnout": 70.2, "d_share": 24.5, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Eagle 20": {"registered": 2216, "turnout": 65.1, "d_share": 25.9, "area": "Zionsville/Whitestown", "township": "Eagle"},
    "Clinton": {"registered": 762, "turnout": 70.7, "d_share": 10.9, "area": "Rural East", "township": "Clinton"},
    "Harrison": {"registered": 567, "turnout": 70.7, "d_share": 8.5, "area": "Rural East", "township": "Harrison"},
    "Marion 1": {"registered": 515, "turnout": 68.9, "d_share": 7.4, "area": "Rural East", "township": "Marion"},
    "Marion 2": {"registered": 556, "turnout": 70.7, "d_share": 8.2, "area": "Rural East", "township": "Marion"},
    "Jackson 1": {"registered": 789, "turnout": 65.7, "d_share": 7.2, "area": "Rural West", "township": "Jackson"},
    "Jackson 2": {"registered": 740, "turnout": 66.6, "d_share": 11.5, "area": "Rural West", "township": "Jackson"},
    "Jackson 3": {"registered": 581, "turnout": 64.9, "d_share": 6.9, "area": "Rural West", "township": "Jackson"},
    "Sugar Creek 1": {"registered": 804, "turnout": 63.4, "d_share": 10.6, "area": "Rural West", "township": "Sugar Creek"},
    "Sugar Creek 2": {"registered": 883, "turnout": 63.4, "d_share": 9.2, "area": "Rural West", "township": "Sugar Creek"},
    "Union 1": {"registered": 1564, "turnout": 67.7, "d_share": 17.9, "area": "Rural West", "township": "Union"},
    "Union 2": {"registered": 1383, "turnout": 69.9, "d_share": 14.6, "area": "Rural West", "township": "Union"},
    "Worth 1": {"registered": 2995, "turnout": 63.0, "d_share": 20.6, "area": "Rural West", "township": "Worth"},
    "Worth 2": {"registered": 2345, "turnout": 73.3, "d_share": 19.3, "area": "Rural West", "township": "Worth"},
    "Perry": {"registered": 1779, "turnout": 72.3, "d_share": 16.4, "area": "Central Rural", "township": "Perry"},
}


# =============================================================================
# AREA DEMOGRAPHICS (from Census ACS 5-Year)
# =============================================================================

AREA_DEMOGRAPHICS = {
    "Lebanon": {
        "median_age": 35.5,
        # Age brackets (voters only, 18+)
        "age_18_29": 0.24, "age_30_44": 0.28, "age_45_64": 0.30, "age_65_plus": 0.18,
        "pct_white": 90.0, "pct_black": 2.5, "pct_hispanic": 5.5, "pct_asian": 1.0, "pct_other": 1.0,
        "pct_bachelors": 19.4,
        "median_income": 72471,
        "zip_codes": ["46052"],
        "city": "Lebanon",
    },
    "Zionsville/Whitestown": {
        "median_age": 37.0,
        "age_18_29": 0.14, "age_30_44": 0.36, "age_45_64": 0.32, "age_65_plus": 0.18,
        "pct_white": 87.0, "pct_black": 3.0, "pct_hispanic": 3.5, "pct_asian": 5.0, "pct_other": 1.5,
        "pct_bachelors": 40.2,
        "median_income": 143721,
        "zip_codes": ["46077", "46075"],
        "city": "Zionsville",  # some will be Whitestown
    },
    "Rural West": {
        "median_age": 40.0,
        "age_18_29": 0.16, "age_30_44": 0.24, "age_45_64": 0.34, "age_65_plus": 0.26,
        "pct_white": 95.0, "pct_black": 0.5, "pct_hispanic": 3.0, "pct_asian": 0.5, "pct_other": 1.0,
        "pct_bachelors": 18.0,
        "median_income": 65000,
        "zip_codes": ["46071", "46075"],
        "city": "Thorntown",
    },
    "Rural East": {
        "median_age": 42.0,
        "age_18_29": 0.14, "age_30_44": 0.22, "age_45_64": 0.36, "age_65_plus": 0.28,
        "pct_white": 96.0, "pct_black": 0.5, "pct_hispanic": 2.0, "pct_asian": 0.5, "pct_other": 1.0,
        "pct_bachelors": 15.0,
        "median_income": 60000,
        "zip_codes": ["46050", "46069"],
        "city": "Sheridan",
    },
    "Central Rural": {
        "median_age": 38.0,
        "age_18_29": 0.16, "age_30_44": 0.30, "age_45_64": 0.32, "age_65_plus": 0.22,
        "pct_white": 92.0, "pct_black": 1.5, "pct_hispanic": 4.0, "pct_asian": 1.5, "pct_other": 1.0,
        "pct_bachelors": 30.0,
        "median_income": 80000,
        "zip_codes": ["46052", "46075"],
        "city": "Lebanon",
    },
}

# City overrides for specific townships
TOWNSHIP_CITY = {
    "Center": "Lebanon",
    "Jefferson": "Lebanon",
    "Washington": "Lebanon",
    "Eagle": "Zionsville",
    "Jackson": "Thorntown",
    "Sugar Creek": "Thorntown",
    "Union": "Westfield",
    "Worth": "Whitestown",
    "Clinton": "Sheridan",
    "Harrison": "Sheridan",
    "Marion": "Sheridan",
    "Perry": "Lebanon",
}


# =============================================================================
# PARTY PRIMARY PULL RATES (Indiana = open primary)
# =============================================================================
# "Party" in Indiana voter files = which primary ballot they last pulled.
# Rates calibrated so aggregated D share roughly matches real vote shares.
# Among primary voters: what % pull D vs R

AREA_D_PRIMARY_RATE = {
    "Lebanon": 0.12,
    "Zionsville/Whitestown": 0.24,
    "Rural West": 0.10,
    "Rural East": 0.06,
    "Central Rural": 0.14,
}

# What fraction of registered voters have ever voted in a primary
AREA_PRIMARY_PARTICIPATION = {
    "Lebanon": 0.50,
    "Zionsville/Whitestown": 0.58,
    "Rural West": 0.52,
    "Rural East": 0.55,
    "Central Rural": 0.54,
}


# =============================================================================
# ELECTION TURNOUT TARGETS (county-wide averages)
# =============================================================================

ELECTIONS = {
    "General2024": {"date": "2024-11-05", "avg_turnout": 0.654, "type": "general"},
    "Primary2024": {"date": "2024-05-07", "avg_turnout": 0.208, "type": "primary"},
    "General2022": {"date": "2022-11-08", "avg_turnout": 0.441, "type": "general"},
    "Primary2022": {"date": "2022-05-03", "avg_turnout": 0.213, "type": "primary"},
    "General2020": {"date": "2020-11-03", "avg_turnout": 0.720, "type": "general"},
    "Primary2020": {"date": "2020-06-02", "avg_turnout": 0.170, "type": "primary"},
    "General2018": {"date": "2018-11-06", "avg_turnout": 0.564, "type": "general"},
    "Primary2018": {"date": "2018-05-08", "avg_turnout": 0.229, "type": "primary"},
}


# =============================================================================
# LEGISLATIVE DISTRICTS (from 2024 race results data)
# =============================================================================

# All of Boone County is US House District 4 (post-2020 redistricting)
# State House and Senate vary by precinct

STATE_HOUSE_MAP = {
    # District 25: Eagle precincts + some Center/Worth/Perry
    25: [
        "Center 05", "Center 16",
        "Eagle 01", "Eagle 02", "Eagle 03", "Eagle 04", "Eagle 05",
        "Eagle 06", "Eagle 07", "Eagle 08", "Eagle 09", "Eagle 10",
        "Eagle 11", "Eagle 12", "Eagle 13", "Eagle 14", "Eagle 15",
        "Eagle 16", "Eagle 17", "Eagle 18", "Eagle 19", "Eagle 20",
        "Perry", "Worth 1", "Worth 2",
    ],
    # District 41: Lebanon precincts + some rural
    41: [
        "Center 01", "Center 02", "Center 03", "Center 04",
        "Center 06", "Center 07", "Center 08", "Center 09",
        "Center 10", "Center 11", "Center 12", "Center 13",
        "Center 14", "Center 15", "Center 17",
        "Clinton", "Jefferson",
        "Sugar Creek 1", "Sugar Creek 2", "Washington",
    ],
    # District 28: rural precincts
    28: [
        "Jackson 1", "Jackson 2", "Jackson 3",
        "Harrison", "Marion 1", "Marion 2",
        "Union 1", "Union 2",
    ],
}

STATE_SENATE_MAP = {
    # District 29: Zionsville/Eagle area
    29: [
        "Eagle 01", "Eagle 02", "Eagle 03", "Eagle 04", "Eagle 05",
        "Eagle 06", "Eagle 07", "Eagle 08", "Eagle 09", "Eagle 10",
        "Eagle 11", "Eagle 12", "Eagle 13", "Eagle 14", "Eagle 15",
        "Eagle 16", "Eagle 17", "Eagle 18", "Eagle 19", "Eagle 20",
        "Perry", "Worth 1", "Worth 2",
    ],
    # District 23: Lebanon and rural
    23: [
        "Center 01", "Center 02", "Center 03", "Center 04", "Center 05",
        "Center 06", "Center 07", "Center 08", "Center 09", "Center 10",
        "Center 11", "Center 12", "Center 13", "Center 14", "Center 15",
        "Center 16", "Center 17",
        "Clinton", "Harrison", "Jackson 1", "Jackson 2", "Jackson 3",
        "Jefferson", "Marion 1", "Marion 2",
        "Sugar Creek 1", "Sugar Creek 2",
        "Union 1", "Union 2", "Washington",
    ],
}


# Build reverse lookups
def _build_district_lookup(district_map):
    lookup = {}
    for district, precincts in district_map.items():
        for p in precincts:
            lookup[p] = district
    return lookup

HOUSE_LOOKUP = _build_district_lookup(STATE_HOUSE_MAP)
SENATE_LOOKUP = _build_district_lookup(STATE_SENATE_MAP)


# =============================================================================
# STREET NAMES BY AREA (realistic Boone County addresses)
# =============================================================================

STREETS = {
    "Lebanon": [
        "N Lebanon St", "S Lebanon St", "E Washington St", "W Washington St",
        "N Meridian St", "S Meridian St", "E Main St", "W Main St",
        "Grant St", "Elm St", "Maple Ave", "Park Ave", "Hendricks Ave",
        "Pearl St", "Center St", "N East St", "S East St",
        "Indianapolis Ave", "Ulen Dr", "Lafayette Ave",
        "N West St", "S West St", "Superior St", "Franklin St",
        "Walnut St", "Oak St", "SR 32", "SR 39",
        "CR 300 S", "CR 200 N", "CR 100 W", "CR 250 E",
    ],
    "Zionsville/Whitestown": [
        "Main St", "Oak St", "Elm St", "Mulberry St", "Sycamore St",
        "Ironstone Dr", "Brookside Pkwy", "Village Dr", "Eagle Crossing Dr",
        "Whitestown Pkwy", "Ansley Blvd", "Hawthorne Dr", "Cobblestone Dr",
        "Maple St", "Poplar St", "Willow Rd", "Pine Crest Dr",
        "Harvest Moon Dr", "Autumn Blaze Ln", "Traders Point Ln",
        "Michigan Rd", "Zionsville Rd", "CR 875 E", "CR 700 N",
        "Starkey Rd", "Ford Rd", "Oak Ridge Dr", "Pebble Brook Ln",
        "Acorn Dr", "Berkshire Blvd", "Countryside Ln", "Whitestown Way",
    ],
    "Rural West": [
        "CR 200 N", "CR 400 E", "CR 100 S", "CR 600 W", "CR 300 N",
        "CR 500 S", "CR 800 E", "CR 150 W", "CR 450 N", "CR 250 S",
        "SR 32", "SR 47", "Old SR 32", "N 400 W", "S 200 E",
        "Thorntown Rd", "Sugar Creek Rd", "Indianapolis Rd",
        "W 300 N", "E 600 S", "N 500 E", "S 100 W",
    ],
    "Rural East": [
        "CR 200 E", "CR 300 N", "CR 100 W", "CR 600 S", "CR 500 E",
        "CR 400 N", "CR 150 S", "CR 250 W", "CR 350 E", "CR 450 S",
        "SR 38", "SR 47", "Sheridan Rd", "Boxley Rd",
        "N 600 E", "S 300 W", "E 200 N", "W 400 S",
    ],
    "Central Rural": [
        "CR 300 E", "CR 200 S", "CR 500 N", "CR 400 W", "CR 100 E",
        "SR 32", "SR 39", "Perry Worth Rd", "Hazelrigg Rd",
        "N 300 W", "S 500 E", "Indianapolis Rd",
    ],
}


# =============================================================================
# VOTER GENERATION FUNCTIONS
# =============================================================================

def generate_age(area_demo):
    """Generate a realistic voter age and date of birth based on area demographics."""
    brackets = [
        (18, 29, area_demo["age_18_29"]),
        (30, 44, area_demo["age_30_44"]),
        (45, 64, area_demo["age_45_64"]),
        (65, 95, area_demo["age_65_plus"]),
    ]
    weights = [b[2] for b in brackets]
    bracket = random.choices(brackets, weights=weights, k=1)[0]

    low, high = bracket[0], bracket[1]
    if high == 95:
        # Taper off: most 65+ voters are 65-80, fewer above
        age = int(low + abs(random.gauss(0, 7)))
        age = max(low, min(age, 99))
    else:
        age = random.randint(low, high)

    # Convert to date of birth
    today = date.today()
    birth_year = today.year - age
    try:
        dob = date(birth_year, random.randint(1, 12), random.randint(1, 28))
    except ValueError:
        dob = date(birth_year, 6, 15)

    return dob, age


def generate_registration_date(age, dob, area):
    """Generate a plausible voter registration date."""
    today = date.today()

    # Most people register between 18-22
    # Newer areas (Zionsville/Whitestown) have more recent registrations
    turned_18 = date(dob.year + 18, min(dob.month, 12), min(dob.day, 28))
    if turned_18 > today:
        turned_18 = today - timedelta(days=30)

    if area == "Zionsville/Whitestown" and random.random() < 0.40:
        # Many moved here recently — registered in last 10 years
        years_ago = random.randint(0, 10)
        reg = today - timedelta(days=years_ago * 365 + random.randint(0, 364))
    elif random.random() < 0.60:
        # Registered shortly after turning 18
        delay_days = random.randint(0, 365 * 4)
        reg = turned_18 + timedelta(days=delay_days)
    else:
        # Registered at some random point in adulthood
        days_since_18 = (today - turned_18).days
        if days_since_18 > 0:
            reg = turned_18 + timedelta(days=random.randint(0, days_since_18))
        else:
            reg = today - timedelta(days=30)

    # Clamp to reasonable range
    if reg > today:
        reg = today - timedelta(days=random.randint(30, 365))
    if reg < date(1960, 1, 1):
        reg = date(1960 + random.randint(0, 20), random.randint(1, 12), random.randint(1, 28))

    return reg


def generate_vote_history(age, dob, reg_date, precinct_turnout_2024, area, propensity):
    """
    Generate Y/N vote history for 8 elections.

    Uses a persistent propensity score so the same voter is consistently
    high or low turnout. Propensity is adjusted by election type and year.
    """
    history = {}

    # Determine if this voter is a "primary voter" — someone who participates
    # in primaries. ~55% of registered voters have pulled a primary ballot at
    # some point. Primary voters have a per-election primary turnout rate of ~40%.
    primary_participation = AREA_PRIMARY_PARTICIPATION.get(area, 0.55)
    is_primary_voter = (propensity + random.gauss(0, 0.15)) > (1.0 - primary_participation)

    for election_name, info in ELECTIONS.items():
        edate = datetime.strptime(info["date"], "%Y-%m-%d").date()

        # Can't vote before registration or before turning 18
        turned_18 = date(dob.year + 18, min(dob.month, 12), min(dob.day, 28))
        if edate < reg_date or edate < turned_18:
            history[election_name] = ""
            continue

        if info["type"] == "general":
            # Base probability = county average turnout for this election
            base = info["avg_turnout"]

            # Adjust for precinct (higher turnout precincts = higher base)
            precinct_factor = (precinct_turnout_2024 / 100.0) / 0.654
            base *= precinct_factor
            base = min(base, 0.95)

            # Threshold model: vote if propensity > (1 - base) + noise
            noise = random.gauss(0, 0.12)
            threshold = 1.0 - base
            voted = (propensity + noise) > threshold

        else:
            # Primary election: only primary voters participate, and even they
            # don't vote in every primary. Per-election rate among primary voters ~40%.
            if is_primary_voter:
                noise = random.gauss(0, 0.12)
                voted = (propensity + noise) > 0.65
            else:
                voted = False

        history[election_name] = "Y" if voted else ""

    return history


def generate_party(d_share, area, vote_history, propensity):
    """
    Assign party based on primary pull history.

    In Indiana, 'party' = which primary ballot you last pulled.
    If you never voted in a primary, party = blank.
    """
    # Check if this voter ever voted in a primary
    primary_elections = ["Primary2024", "Primary2022", "Primary2020", "Primary2018"]
    voted_primary = any(vote_history.get(p) == "Y" for p in primary_elections)

    if not voted_primary:
        return ""  # Unaffiliated — never pulled a primary ballot

    # Among primary voters, determine D vs R pull
    # Base rate from area, adjusted by precinct D share
    area_d_rate = AREA_D_PRIMARY_RATE[area]

    # Adjust: precincts with higher D share get higher D primary pull rate
    area_avg_d = {
        "Lebanon": 13.5, "Zionsville/Whitestown": 24.5,
        "Rural West": 13.5, "Rural East": 8.8, "Central Rural": 16.4,
    }
    d_share_diff = d_share - area_avg_d.get(area, 15.0)
    adjusted_d_rate = area_d_rate + (d_share_diff * 0.008)
    adjusted_d_rate = max(0.03, min(adjusted_d_rate, 0.50))

    if random.random() < adjusted_d_rate:
        return "D"
    else:
        return "R"


def generate_address(area, township, fake):
    """Generate a realistic Boone County street address."""
    area_streets = STREETS.get(area, STREETS["Lebanon"])
    street = random.choice(area_streets)

    # House number ranges vary by area
    if area == "Zionsville/Whitestown":
        num = random.randint(1000, 15000)
    elif area == "Lebanon":
        num = random.randint(100, 2500)
    else:
        num = random.randint(100, 12000)

    # County roads use different format
    if street.startswith("CR ") or street.startswith("SR "):
        return f"{num} {street}"
    else:
        return f"{num} {street}"


def get_districts(precinct_name):
    """Return (congressional, state_senate, state_house) for a precinct."""
    cd = 4  # All Boone County is IN-4
    ss = SENATE_LOOKUP.get(precinct_name, 23)
    sh = HOUSE_LOOKUP.get(precinct_name, 41)
    return cd, ss, sh


def generate_propensity(age, area):
    """
    Generate a persistent voter propensity score (0.0 - 1.0).
    Higher = more likely to vote in all elections.
    """
    base = random.gauss(0.50, 0.20)

    # Age adjustment: older voters more likely to vote
    if age >= 65:
        base += 0.12
    elif age >= 45:
        base += 0.06
    elif age <= 25:
        base -= 0.12
    elif age <= 35:
        base -= 0.04

    # Area adjustment: suburban areas have slightly higher propensity
    if area == "Zionsville/Whitestown":
        base += 0.04
    elif area in ("Rural East", "Central Rural"):
        base += 0.02

    return max(0.0, min(1.0, base))


# =============================================================================
# MAIN GENERATOR
# =============================================================================

def generate_single_voter(vanid, precinct_name, precinct_info, area_demo, fake):
    """Generate one complete synthetic voter record."""
    area = precinct_info["area"]
    township = precinct_info["township"]

    # 1. Demographics
    gender = random.choices(["F", "M"], weights=[0.51, 0.49])[0]
    dob, age = generate_age(area_demo)

    if gender == "F":
        first = fake.first_name_female()
    else:
        first = fake.first_name_male()
    last = fake.last_name()
    middle = fake.first_name()[0] if random.random() < 0.70 else ""
    suffix = random.choice(["Jr", "Sr", "II", "III"]) if random.random() < 0.03 else ""

    # 2. Registration
    reg_date = generate_registration_date(age, dob, area)
    reg_status = "Active" if random.random() < 0.95 else "Inactive"

    # 3. Propensity (persistent engagement score)
    propensity = generate_propensity(age, area)

    # 4. Vote history
    vote_history = generate_vote_history(
        age, dob, reg_date,
        precinct_info["turnout"],
        area,
        propensity,
    )

    # 5. Party (based on primary pull)
    party = generate_party(
        precinct_info["d_share"],
        area,
        vote_history,
        propensity,
    )

    # 6. Address
    address = generate_address(area, township, fake)
    city = TOWNSHIP_CITY.get(township, area_demo.get("city", "Lebanon"))
    zipcode = random.choice(area_demo["zip_codes"])

    # 7. Contact info (sparse — realistic fill rates)
    phone = fake.phone_number() if random.random() < 0.35 else ""
    email = fake.email() if random.random() < 0.20 else ""

    # 8. Districts
    cd, ss, sh = get_districts(precinct_name)

    return {
        "VANID": vanid,
        "LastName": last,
        "FirstName": first,
        "MiddleName": middle,
        "Suffix": suffix,
        "DateOfBirth": dob.strftime("%m/%d/%Y"),
        "Gender": gender,
        "Age": age,
        "AddressLine1": address,
        "City": city,
        "State": "IN",
        "Zip": zipcode,
        "PrecinctName": precinct_name,
        "Township": township,
        "CongressionalDistrict": cd,
        "StateSenateDistrict": ss,
        "StateHouseDistrict": sh,
        "Party": party,
        "RegistrationDate": reg_date.strftime("%m/%d/%Y"),
        "RegistrationStatus": reg_status,
        "General2024": vote_history.get("General2024", ""),
        "Primary2024": vote_history.get("Primary2024", ""),
        "General2022": vote_history.get("General2022", ""),
        "Primary2022": vote_history.get("Primary2022", ""),
        "General2020": vote_history.get("General2020", ""),
        "Primary2020": vote_history.get("Primary2020", ""),
        "General2018": vote_history.get("General2018", ""),
        "Primary2018": vote_history.get("Primary2018", ""),
        "Phone": phone,
        "Email": email,
    }


def generate_voters(seed=42, count=None, output_path=None):
    """
    Generate the full synthetic voter file.

    Args:
        seed: Random seed for reproducibility
        count: If set, scale down to this many total voters
        output_path: Where to write the CSV
    """
    fake = Faker("en_US")
    Faker.seed(seed)
    random.seed(seed)

    if output_path is None:
        output_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data", "synthetic_voter_file.csv"
        )

    # Calculate total and scaling factor
    total_real = sum(p["registered"] for p in PRECINCT_DATA.values())
    scale = count / total_real if count else 1.0

    fieldnames = [
        "VANID", "LastName", "FirstName", "MiddleName", "Suffix",
        "DateOfBirth", "Gender", "Age",
        "AddressLine1", "City", "State", "Zip",
        "PrecinctName", "Township", "CongressionalDistrict",
        "StateSenateDistrict", "StateHouseDistrict",
        "Party", "RegistrationDate", "RegistrationStatus",
        "General2024", "Primary2024", "General2022", "Primary2022",
        "General2020", "Primary2020", "General2018", "Primary2018",
        "Phone", "Email",
    ]

    vanid = 100000
    total_written = 0

    # Track stats for summary
    stats = {
        "gender": {"F": 0, "M": 0},
        "party": {"R": 0, "D": 0, "": 0},
        "area": {},
        "age_sum": 0,
        "elections": {e: 0 for e in ELECTIONS},
    }

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for precinct_name, precinct_info in PRECINCT_DATA.items():
            area = precinct_info["area"]
            area_demo = AREA_DEMOGRAPHICS[area]

            n_voters = max(1, round(precinct_info["registered"] * scale))

            for i in range(n_voters):
                vanid += 1
                voter = generate_single_voter(
                    vanid, precinct_name, precinct_info, area_demo, fake
                )
                writer.writerow(voter)
                total_written += 1

                # Track stats
                stats["gender"][voter["Gender"]] += 1
                stats["party"][voter["Party"]] += 1
                stats["area"][area] = stats["area"].get(area, 0) + 1
                stats["age_sum"] += voter["Age"]
                for e in ELECTIONS:
                    if voter.get(e) == "Y":
                        stats["elections"][e] += 1

                # Progress
                if total_written % 10000 == 0:
                    print(f"  Generated {total_written:,} voters...")

    print_summary(output_path, total_written, stats)
    return output_path


def print_summary(output_path, total, stats):
    """Print validation summary after generation."""
    print(f"\n{'='*60}")
    print(f"  SYNTHETIC VOTER FILE GENERATED")
    print(f"{'='*60}")
    print(f"  Output:    {output_path}")
    print(f"  Total:     {total:,} voter records")
    print(f"  Precincts: {len(PRECINCT_DATA)}")
    print()

    # Area distribution
    print("  Area Distribution:")
    for area, count in sorted(stats["area"].items(), key=lambda x: -x[1]):
        pct = count / total * 100
        print(f"    {area:30s} {count:>6,}  ({pct:.1f}%)")
    print()

    # Gender
    f_pct = stats["gender"]["F"] / total * 100
    m_pct = stats["gender"]["M"] / total * 100
    print(f"  Gender:    {f_pct:.1f}% F, {m_pct:.1f}% M")

    # Age
    avg_age = stats["age_sum"] / total
    print(f"  Avg Age:   {avg_age:.1f}")
    print()

    # Party
    print("  Party (Primary Pull):")
    for party_label, display in [("R", "Republican"), ("D", "Democratic"), ("", "Unaffiliated")]:
        count = stats["party"][party_label]
        pct = count / total * 100
        print(f"    {display:20s} {count:>6,}  ({pct:.1f}%)")
    print()

    # Vote history rates (% of all registered voters who voted)
    print("  Vote History (% of registered who voted):")
    for ename, info in ELECTIONS.items():
        actual = stats["elections"][ename] / total * 100
        target = info["avg_turnout"] * 100
        diff = actual - target
        print(f"    {ename:16s} {actual:5.1f}%  (target: {target:.1f}%, diff: {diff:+.1f})")
    print("  Note: Older elections show lower rates because younger voters weren't")
    print("  registered yet. This matches real voter file behavior.")
    print()

    # File size
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  File Size: {size_mb:.1f} MB")
    print(f"{'='*60}")


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic Boone County voter file")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--count", type=int, default=None,
                        help="Total voters to generate (default: full ~62K)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output CSV path (default: data/synthetic_voter_file.csv)")
    args = parser.parse_args()

    print(f"Generating synthetic voter file (seed={args.seed})...")
    generate_voters(seed=args.seed, count=args.count, output_path=args.output)
