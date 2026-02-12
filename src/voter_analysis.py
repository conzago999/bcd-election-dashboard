"""
Voter File Analysis Module for BCD Election Dashboard

Reads a VAN-format voter file CSV and provides analysis functions for:
- Voter universe profiling (demographics, party, registration)
- Turnout scoring and propensity classification
- Persuasion target identification
- Precinct-level drill-downs

Works with both synthetic (generated) and real VAN export data.
"""

import os
import pandas as pd
import numpy as np


# =============================================================================
# PRECINCT-TO-AREA MAPPING (matches analysis.py and census_acs.py)
# =============================================================================

PRECINCT_AREA_MAP = {
    "CENTER": "Lebanon",
    "JEFFERSON": "Lebanon",
    "WASHINGTON": "Lebanon",
    "EAGLE": "Zionsville/Whitestown",
    "JACKSON": "Rural West",
    "SUGAR CREEK": "Rural West",
    "SUGAR": "Rural West",
    "UNION": "Rural West",
    "WORTH": "Rural West",
    "CLINTON": "Rural East",
    "HARRISON": "Rural East",
    "MARION": "Rural East",
    "PERRY": "Central Rural",
}

GENERAL_ELECTIONS = ["General2024", "General2022", "General2020", "General2018"]
PRIMARY_ELECTIONS = ["Primary2024", "Primary2022", "Primary2020", "Primary2018"]
ALL_ELECTIONS = GENERAL_ELECTIONS + PRIMARY_ELECTIONS


def _map_precinct_to_area(precinct_name):
    """Map a precinct name to its geographic area."""
    upper = str(precinct_name).upper().strip()
    for prefix, area in PRECINCT_AREA_MAP.items():
        if upper.startswith(prefix):
            return area
    return "Unknown"


# =============================================================================
# FILE LOADING
# =============================================================================

def load_voter_file(path=None):
    """
    Load a voter file CSV (synthetic or real VAN export).

    Returns a DataFrame with added computed columns:
        - area: geographic area
        - turnout_score: 0-8 count of elections voted in
        - general_score: 0-4 count of general elections voted in
        - primary_score: 0-4 count of primaries voted in
        - voter_type: classification (Super, Regular, Occasional, Inactive, New)
        - age_group: bracket (18-29, 30-44, 45-64, 65+)
    """
    if path is None:
        path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data", "synthetic_voter_file.csv"
        )

    if not os.path.exists(path):
        return pd.DataFrame()

    df = pd.read_csv(path, dtype={"Zip": str, "Phone": str})

    # Add area
    df["Area"] = df["PrecinctName"].apply(_map_precinct_to_area)

    # Compute turnout scores
    for col in ALL_ELECTIONS:
        if col not in df.columns:
            df[col] = ""

    df["general_score"] = sum((df[e] == "Y").astype(int) for e in GENERAL_ELECTIONS)
    df["primary_score"] = sum((df[e] == "Y").astype(int) for e in PRIMARY_ELECTIONS)
    df["turnout_score"] = df["general_score"] + df["primary_score"]

    # Voter type classification
    def classify_voter(row):
        gs = row["general_score"]
        if gs == 4:
            return "Super Voter"      # Voted in all 4 generals
        elif gs == 3:
            return "Regular"           # Voted in 3 of 4
        elif gs >= 1:
            return "Occasional"        # Voted in 1-2
        else:
            return "Inactive"          # Never voted in a general
    df["voter_type"] = df.apply(classify_voter, axis=1)

    # Age groups
    df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
    def age_group(age):
        if pd.isna(age):
            return "Unknown"
        elif age < 30:
            return "18-29"
        elif age < 45:
            return "30-44"
        elif age < 65:
            return "45-64"
        else:
            return "65+"
    df["age_group"] = df["Age"].apply(age_group)

    # Clean party
    df["Party"] = df["Party"].fillna("").astype(str).str.strip()
    df.loc[~df["Party"].isin(["D", "R"]), "Party"] = ""

    return df


# =============================================================================
# ANALYSIS FUNCTIONS
# =============================================================================

def get_voter_universe_summary(df):
    """
    Top-level summary of the voter universe.

    Returns dict with:
        - total, active, inactive counts
        - party breakdown
        - area breakdown
        - age group breakdown
        - voter type breakdown
    """
    if df.empty:
        return {}

    active = df[df["RegistrationStatus"] == "Active"]

    return {
        "total": len(df),
        "active": len(active),
        "inactive": len(df) - len(active),
        "party": df["Party"].value_counts().to_dict(),
        "area": df["Area"].value_counts().to_dict(),
        "age_group": df["age_group"].value_counts().to_dict(),
        "voter_type": df["voter_type"].value_counts().to_dict(),
        "gender": df["Gender"].value_counts().to_dict(),
        "avg_age": round(df["Age"].mean(), 1),
        "avg_turnout_score": round(df["turnout_score"].mean(), 1),
        "avg_general_score": round(df["general_score"].mean(), 1),
    }


def get_turnout_scored_voters(df):
    """
    Return DataFrame of voters scored by turnout propensity.

    Adds columns:
        - propensity_label: High/Medium/Low/None
        - surge_2020: voted in 2020 general but not 2022 (mobilization targets)
        - dropoff: voted in 2018 general but not 2022 or 2024 (lapsed voters)
    """
    if df.empty:
        return df

    scored = df.copy()

    # Propensity label
    def propensity_label(gs):
        if gs >= 3:
            return "High"
        elif gs >= 2:
            return "Medium"
        elif gs >= 1:
            return "Low"
        else:
            return "None"
    scored["propensity"] = scored["general_score"].apply(propensity_label)

    # Surge voters: voted in 2020 (high-turnout presidential) but NOT in 2022 (midterm)
    scored["surge_2020"] = (
        (scored["General2020"] == "Y") &
        (scored["General2022"] != "Y")
    )

    # Drop-off voters: voted in 2018 but not in 2022 or 2024
    scored["dropoff"] = (
        (scored["General2018"] == "Y") &
        (scored["General2022"] != "Y") &
        (scored["General2024"] != "Y")
    )

    # Recent activators: didn't vote in 2018 or 2020 but did in 2022 or 2024
    scored["new_activator"] = (
        (scored["General2018"] != "Y") &
        (scored["General2020"] != "Y") &
        ((scored["General2022"] == "Y") | (scored["General2024"] == "Y"))
    )

    return scored


def get_persuasion_targets(df):
    """
    Identify persuasion targets: unaffiliated or soft-R voters in competitive
    precincts who vote frequently enough to be worth contacting.

    Returns DataFrame of target voters with priority scoring.
    """
    if df.empty:
        return df

    scored = get_turnout_scored_voters(df)

    # Target universe: voters who are
    # 1. Unaffiliated (never pulled a primary) OR pulled D primary
    # 2. Active registration
    # 3. Voted in at least 1 general election (worth contacting)
    # 4. In precincts with D share > 15% (not wasting time in 7% precincts)

    # We don't have D share in the voter file directly, but we can estimate
    # from the area. Use competitive areas.
    competitive_areas = ["Zionsville/Whitestown", "Central Rural"]
    moderate_areas = ["Lebanon", "Rural West"]

    targets = scored[
        (scored["RegistrationStatus"] == "Active") &
        (scored["general_score"] >= 1) &
        (scored["Party"].isin(["", "D"]))
    ].copy()

    # Priority scoring
    def priority_score(row):
        score = 0
        # Area value
        if row["Area"] in competitive_areas:
            score += 3
        elif row["Area"] in moderate_areas:
            score += 1

        # Turnout consistency
        score += row["general_score"]

        # Surge voter bonus (voted 2020 but not 2022 — mobilizable)
        if row.get("surge_2020", False):
            score += 2

        # D primary puller bonus
        if row["Party"] == "D":
            score += 2

        # Age bonus (younger voters = longer-term investment)
        if row["Age"] < 35:
            score += 1

        return score

    targets["priority_score"] = targets.apply(priority_score, axis=1)
    targets = targets.sort_values("priority_score", ascending=False)

    return targets


def get_precinct_voter_profile(df, precinct_name):
    """
    Detailed voter profile for a single precinct.

    Returns dict with demographics, party, turnout, and voter type breakdowns.
    """
    if df.empty:
        return {}

    pct = df[df["PrecinctName"] == precinct_name]
    if pct.empty:
        return {}

    active = pct[pct["RegistrationStatus"] == "Active"]

    return {
        "precinct": precinct_name,
        "total_voters": len(pct),
        "active": len(active),
        "area": pct["Area"].iloc[0] if len(pct) > 0 else "Unknown",
        "party_breakdown": pct["Party"].value_counts().to_dict(),
        "d_primary_pct": round(
            len(pct[pct["Party"] == "D"]) /
            max(len(pct[pct["Party"].isin(["D", "R"])]), 1) * 100, 1
        ),
        "age_breakdown": pct["age_group"].value_counts().to_dict(),
        "avg_age": round(pct["Age"].mean(), 1),
        "gender_breakdown": pct["Gender"].value_counts().to_dict(),
        "voter_type_breakdown": pct["voter_type"].value_counts().to_dict(),
        "avg_general_score": round(pct["general_score"].mean(), 1),
        "avg_turnout_score": round(pct["turnout_score"].mean(), 1),
        "general_2024_turnout": round(
            (pct["General2024"] == "Y").sum() / max(len(pct), 1) * 100, 1
        ),
        "surge_2020_count": int(
            ((pct["General2020"] == "Y") & (pct["General2022"] != "Y")).sum()
        ),
        "has_phone_pct": round(
            pct["Phone"].notna().sum() / max(len(pct), 1) * 100, 1
        ) if "Phone" in pct.columns else 0,
        "has_email_pct": round(
            pct["Email"].notna().sum() / max(len(pct), 1) * 100, 1
        ) if "Email" in pct.columns else 0,
    }


def get_area_voter_summary(df):
    """
    Aggregate voter file data by geographic area.

    Returns DataFrame with one row per area.
    """
    if df.empty:
        return pd.DataFrame()

    areas = []
    for area_name, group in df.groupby("Area"):
        if area_name == "Unknown":
            continue
        active = group[group["RegistrationStatus"] == "Active"]
        d_count = len(group[group["Party"] == "D"])
        r_count = len(group[group["Party"] == "R"])
        dr_total = d_count + r_count

        areas.append({
            "Area": area_name,
            "Total Voters": len(group),
            "Active": len(active),
            "D Primary": d_count,
            "R Primary": r_count,
            "Unaffiliated": len(group) - d_count - r_count,
            "D% (of primary pullers)": round(d_count / max(dr_total, 1) * 100, 1),
            "Avg Age": round(group["Age"].mean(), 1),
            "Avg General Score": round(group["general_score"].mean(), 1),
            "Super Voters": len(group[group["voter_type"] == "Super Voter"]),
            "Surge 2020": int(
                ((group["General2020"] == "Y") & (group["General2022"] != "Y")).sum()
            ),
            "Has Phone %": round(
                group["Phone"].notna().sum() / max(len(group), 1) * 100, 1
            ) if "Phone" in group.columns else 0,
        })

    return pd.DataFrame(areas).sort_values("Total Voters", ascending=False)
