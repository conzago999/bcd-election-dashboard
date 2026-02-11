"""
BCD Census ACS Module
Fetches demographic data from the US Census Bureau ACS 5-Year API
and maps it to Boone County townships for correlation with election data.

Census API docs: https://www.census.gov/data/developers/data-sets.html
Boone County FIPS: State=18, County=011
"""

import requests
import pandas as pd
import numpy as np
import os

# Boone County FIPS
STATE_FIPS = "18"
COUNTY_FIPS = "011"

# ACS 5-Year variable codes
ACS_VARIABLES = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_income",
    "B01002_001E": "median_age",
    "B15003_022E": "bachelors_degree",
    "B15003_001E": "education_universe",
    "B02001_002E": "white_alone",
    "B02001_003E": "black_alone",
    "B03003_003E": "hispanic",
    "B25077_001E": "median_home_value",
    "B25003_001E": "housing_tenure_total",
    "B25003_002E": "owner_occupied",
    # Age 65+ (male + female buckets)
    "B01001_020E": "male_65_66",
    "B01001_021E": "male_67_69",
    "B01001_022E": "male_70_74",
    "B01001_023E": "male_75_79",
    "B01001_024E": "male_80_84",
    "B01001_025E": "male_85plus",
    "B01001_044E": "female_65_66",
    "B01001_045E": "female_67_69",
    "B01001_046E": "female_70_74",
    "B01001_047E": "female_75_79",
    "B01001_048E": "female_80_84",
    "B01001_049E": "female_85plus",
}

# Census tract → Township mapping for Boone County
# Based on geographic overlap of census tracts with Indiana civil townships
# Tracts that span multiple townships are assigned to the dominant one
TRACT_TOWNSHIP_MAP = {
    "810100": "Rural West",       # Thorntown/Dover area — Jackson, Sugar Creek, Union, Worth townships
    "810200": "Rural East",       # Sheridan area — Marion, Clinton, Harrison townships
    "810300": "Lebanon West",     # Lebanon west side
    "810400": "Lebanon Core",     # Lebanon city center
    "810500": "Lebanon East",     # Lebanon east side
    "810601": "Whitestown",       # Whitestown / south Zionsville border
    "810604": "Zionsville Core",  # Zionsville downtown / central
    "810605": "Zionsville East",  # Zionsville east side
    "810606": "Zionsville West",  # Zionsville / Whitestown new development
    "810607": "Whitestown Core",  # Whitestown proper
    "810700": "Central Rural",    # Between Lebanon & Zionsville — Perry area
}

# Precinct name prefix → Township area mapping (for election data aggregation)
# This maps precinct naming conventions to the same area labels used in TRACT_TOWNSHIP_MAP
PRECINCT_AREA_MAP = {
    "CENTER": "Lebanon",
    "JEFFERSON": "Lebanon",
    "WASHINGTON": "Lebanon",
    "EAGLE": "Zionsville/Whitestown",
    "JACKSON": "Rural West",
    "MARION": "Rural East",
    "PERRY": "Central Rural",
    "SUGAR CREEK": "Rural West",
    "UNION": "Rural West",
    "WORTH": "Rural West",
    "CLINTON": "Rural East",
    "HARRISON": "Rural East",
}

# For the demographic tab, we group tracts into broader areas that align with
# how precincts cluster. This allows meaningful correlation analysis.
TRACT_AREA_MAP = {
    "810100": "Rural West",
    "810200": "Rural East",
    "810300": "Lebanon",
    "810400": "Lebanon",
    "810500": "Lebanon",
    "810601": "Zionsville/Whitestown",
    "810604": "Zionsville/Whitestown",
    "810605": "Zionsville/Whitestown",
    "810606": "Zionsville/Whitestown",
    "810607": "Zionsville/Whitestown",
    "810700": "Central Rural",
}


def fetch_acs_data(api_key, year=2022):
    """
    Fetch ACS 5-Year demographic data for all Boone County census tracts.

    Args:
        api_key: Census Bureau API key
        year: ACS 5-year data year (default 2022, the latest)

    Returns:
        DataFrame with one row per tract, demographic columns computed
    """
    var_codes = list(ACS_VARIABLES.keys())
    var_string = "NAME," + ",".join(var_codes)

    url = f"https://api.census.gov/data/{year}/acs/acs5"
    params = {
        "get": var_string,
        "for": "tract:*",
        "in": f"state:{STATE_FIPS}+county:{COUNTY_FIPS}",
        "key": api_key,
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data[1:], columns=data[0])

    # Rename columns to friendly names
    rename_map = {code: name for code, name in ACS_VARIABLES.items()}
    df = df.rename(columns=rename_map)

    # Convert numeric columns
    numeric_cols = list(ACS_VARIABLES.values())
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Compute derived metrics
    df["pct_bachelors"] = (df["bachelors_degree"] / df["education_universe"] * 100).round(1)
    df["pct_white"] = (df["white_alone"] / df["total_population"] * 100).round(1)
    df["pct_black"] = (df["black_alone"] / df["total_population"] * 100).round(1)
    df["pct_hispanic"] = (df["hispanic"] / df["total_population"] * 100).round(1)
    df["pct_owner_occupied"] = (df["owner_occupied"] / df["housing_tenure_total"] * 100).round(1)

    # Age 65+ total
    age_65_cols = [
        "male_65_66", "male_67_69", "male_70_74", "male_75_79", "male_80_84", "male_85plus",
        "female_65_66", "female_67_69", "female_70_74", "female_75_79", "female_80_84", "female_85plus",
    ]
    df["pop_65plus"] = df[age_65_cols].sum(axis=1)
    df["pct_65plus"] = (df["pop_65plus"] / df["total_population"] * 100).round(1)

    # Add area mapping
    df["area"] = df["tract"].map(TRACT_AREA_MAP)
    df["area_detail"] = df["tract"].map(TRACT_TOWNSHIP_MAP)

    # Clean up: keep useful columns
    keep_cols = [
        "NAME", "tract", "area", "area_detail",
        "total_population", "median_income", "median_age",
        "pct_bachelors", "pct_white", "pct_black", "pct_hispanic",
        "pct_owner_occupied", "pct_65plus", "median_home_value",
        "bachelors_degree", "education_universe", "owner_occupied",
        "housing_tenure_total", "pop_65plus",
    ]
    return df[keep_cols]


def get_area_demographics(api_key, year=2022):
    """
    Aggregate tract-level Census data to area level (Lebanon, Zionsville/Whitestown,
    Rural West, Rural East, Central Rural) for correlation with election data.

    Uses population-weighted averages for rates, sums for counts.
    """
    tracts = fetch_acs_data(api_key, year)

    # Group by area and compute population-weighted stats
    areas = []
    for area_name, group in tracts.groupby("area"):
        total_pop = group["total_population"].sum()
        if total_pop == 0:
            continue

        # Population-weighted averages
        w = group["total_population"] / total_pop
        row = {
            "area": area_name,
            "population": int(total_pop),
            "tracts": len(group),
            "median_income": int((group["median_income"] * w).sum()),
            "median_age": round((group["median_age"] * w).sum(), 1),
            "pct_bachelors": round(
                group["bachelors_degree"].sum() / group["education_universe"].sum() * 100, 1
            ),
            "pct_white": round(group["pct_white"].mean(), 1),  # simple avg ok for %
            "pct_65plus": round(group["pop_65plus"].sum() / total_pop * 100, 1),
            "pct_owner_occupied": round(
                group["owner_occupied"].sum() / group["housing_tenure_total"].sum() * 100, 1
            ),
            "median_home_value": int((group["median_home_value"] * w).sum()),
        }
        areas.append(row)

    result = pd.DataFrame(areas)
    result = result.sort_values("population", ascending=False).reset_index(drop=True)
    return result


def get_tract_detail(api_key, year=2022):
    """
    Get tract-level detail for the Community Profile display.
    Returns all 11 tracts with their demographic data and area labels.
    """
    return fetch_acs_data(api_key, year)


if __name__ == "__main__":
    # Quick test — requires API key as environment variable or argument
    import sys
    key = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CENSUS_API_KEY")
    if not key:
        print("Usage: python census_acs.py <api_key>")
        print("  or set CENSUS_API_KEY environment variable")
        sys.exit(1)

    print("Fetching ACS data for Boone County, IN...")
    tracts = fetch_acs_data(key)
    print(f"\n{len(tracts)} census tracts found:\n")
    print(tracts[["area_detail", "total_population", "median_income", "median_age",
                   "pct_bachelors", "pct_white", "pct_owner_occupied", "median_home_value"]].to_string())

    print("\n\nArea-level aggregation:\n")
    areas = get_area_demographics(key)
    print(areas.to_string())
    print(f"\nTotal county population: {areas['population'].sum():,}")
