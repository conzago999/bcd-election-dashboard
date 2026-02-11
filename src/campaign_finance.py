"""
BCD Campaign Finance Module
Fetches Indiana state campaign finance data from the Indiana Election Division
bulk CSV downloads. Filters for Boone County ZIP codes.

Source: https://campaignfinance.in.gov/PublicSite/Reporting/DataDownload.aspx
Data covers state and local races (governor, state legislature, county offices, etc.)
Federal race data (US Senate, US House, President) is available from FEC separately.
"""

import requests
import zipfile
import io
import pandas as pd
import numpy as np
import os
import re

# Core Boone County ZIP codes (not cross-boundary)
BOONE_COUNTY_ZIPS = [
    "46035",  # Colfax
    "46050",  # Kirklin
    "46052",  # Lebanon (county seat)
    "46069",  # Sheridan
    "46071",  # Thorntown
    "46075",  # Whitestown
    "46077",  # Zionsville
]

# ZIP to area mapping (matches census_acs.py area names)
ZIP_AREA_MAP = {
    "46052": "Lebanon",
    "46075": "Zionsville/Whitestown",
    "46077": "Zionsville/Whitestown",
    "46071": "Rural West",
    "46035": "Rural West",
    "46050": "Rural East",
    "46069": "Rural East",
}

# Known committee party affiliations (for classification)
# We classify by keywords in committee names and known committees
DEM_KEYWORDS = [
    "actblue", "democrat", "democratic", "blue", "harris", "biden",
    "donnelly", "mccormick for indiana",
]
REP_KEYWORDS = [
    "republican", "gop", "braun", "holcomb", "rokita", "indiana republican",
    "trump", "pence", "young for indiana",
]

DOWNLOAD_BASE = "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


def classify_party(committee_name):
    """
    Attempt to classify a committee as D, R, or Unknown based on name keywords.
    """
    if not isinstance(committee_name, str):
        return "Unknown"
    lower = committee_name.lower()
    for kw in DEM_KEYWORDS:
        if kw in lower:
            return "D"
    for kw in REP_KEYWORDS:
        if kw in lower:
            return "R"
    return "Unknown"


def fetch_indiana_contributions(year, timeout=60):
    """
    Download and parse Indiana campaign finance contribution data for a given year.

    Args:
        year: Election year (2018-2024)
        timeout: HTTP request timeout in seconds

    Returns:
        DataFrame with all contributions for that year, or empty DataFrame on failure
    """
    url = f"{DOWNLOAD_BASE}/{year}_ContributionData.csv.zip"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
    except Exception as e:
        print(f"Failed to download {year}: {e}")
        return pd.DataFrame()

    if len(resp.content) < 100:
        return pd.DataFrame()

    try:
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_name = z.namelist()[0]
        df = pd.read_csv(
            z.open(csv_name),
            low_memory=False,
            encoding="latin-1",
            on_bad_lines="skip",
        )
    except Exception as e:
        print(f"Failed to parse {year}: {e}")
        return pd.DataFrame()

    return df


def get_boone_county_contributions(years=None):
    """
    Fetch and filter Indiana campaign finance contributions for Boone County.

    Args:
        years: List of years to fetch (default: 2018-2024 even years)

    Returns:
        DataFrame with Boone County contributions across all requested years
    """
    if years is None:
        years = [2018, 2020, 2022, 2024]

    all_data = []
    for year in years:
        df = fetch_indiana_contributions(year)
        if df.empty:
            continue

        # Clean ZIP codes
        df["zip_clean"] = df["Zip"].astype(str).str[:5]

        # Filter to Boone County
        boone = df[df["zip_clean"].isin(BOONE_COUNTY_ZIPS)].copy()

        if boone.empty:
            continue

        # Add metadata
        boone["year"] = year
        boone["area"] = boone["zip_clean"].map(ZIP_AREA_MAP)
        boone["party_est"] = boone["Committee"].apply(classify_party)

        # Clean amount
        boone["Amount"] = pd.to_numeric(boone["Amount"], errors="coerce").fillna(0)

        # Parse date
        if "ContributionDate" in boone.columns:
            boone["date"] = pd.to_datetime(boone["ContributionDate"], errors="coerce")

        all_data.append(boone)

    if not all_data:
        return pd.DataFrame()

    result = pd.concat(all_data, ignore_index=True)
    return result


def get_contribution_summary(contributions_df=None, years=None):
    """
    Summarize Boone County contributions by year and party.

    Returns:
        DataFrame with year, party breakdown, totals, and donor counts
    """
    if contributions_df is None:
        contributions_df = get_boone_county_contributions(years)

    if contributions_df.empty:
        return pd.DataFrame()

    summary = contributions_df.groupby(["year", "party_est"]).agg(
        total_amount=("Amount", "sum"),
        contribution_count=("Amount", "count"),
        avg_contribution=("Amount", "mean"),
        unique_donors=("Name", "nunique"),
    ).reset_index()

    summary["total_amount"] = summary["total_amount"].round(0)
    summary["avg_contribution"] = summary["avg_contribution"].round(0)

    return summary


def get_contribution_by_area(contributions_df=None, years=None):
    """
    Summarize contributions by geographic area (matching census areas).

    Returns:
        DataFrame with area, year, party breakdown, amounts
    """
    if contributions_df is None:
        contributions_df = get_boone_county_contributions(years)

    if contributions_df.empty:
        return pd.DataFrame()

    by_area = contributions_df.groupby(["area", "year"]).agg(
        total_amount=("Amount", "sum"),
        contribution_count=("Amount", "count"),
        d_amount=("Amount", lambda x: x[contributions_df.loc[x.index, "party_est"] == "D"].sum()),
        r_amount=("Amount", lambda x: x[contributions_df.loc[x.index, "party_est"] == "R"].sum()),
    ).reset_index()

    by_area["d_pct"] = np.where(
        by_area["total_amount"] > 0,
        (by_area["d_amount"] / by_area["total_amount"] * 100).round(1),
        0
    )

    return by_area


def get_top_committees(contributions_df=None, years=None, top_n=15):
    """
    Get top recipient committees for Boone County donors.

    Returns:
        DataFrame with committee name, party estimate, total amount, donor count
    """
    if contributions_df is None:
        contributions_df = get_boone_county_contributions(years)

    if contributions_df.empty:
        return pd.DataFrame()

    top = contributions_df.groupby(["Committee", "party_est"]).agg(
        total_amount=("Amount", "sum"),
        contribution_count=("Amount", "count"),
        unique_donors=("Name", "nunique"),
        years_active=("year", lambda x: ", ".join(sorted(set(x.astype(str))))),
    ).reset_index()

    top = top.sort_values("total_amount", ascending=False).head(top_n)
    top["total_amount"] = top["total_amount"].round(0)
    return top


if __name__ == "__main__":
    print("Fetching Boone County campaign finance data...")
    print("=" * 60)

    contributions = get_boone_county_contributions()
    print(f"\nTotal contributions: {len(contributions):,}")
    print(f"Total amount: ${contributions['Amount'].sum():,.0f}")
    print(f"Years covered: {sorted(contributions['year'].unique())}")

    print("\n--- Summary by Year & Party ---")
    summary = get_contribution_summary(contributions)
    print(summary.to_string(index=False))

    print("\n--- Top Committees ---")
    top = get_top_committees(contributions)
    print(top[["Committee", "party_est", "total_amount", "unique_donors"]].to_string(index=False))
