"""
Indiana Election Analysis — V2 Multi-County Analytics

All functions take a county name and return DataFrames/dicts
suitable for Streamlit display. No hardcoded county-specific data.
"""

import pandas as pd
import numpy as np
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from database import get_connection


# ─── Helpers ───────────────────────────────────────────────────────────────

def _q(sql, params=None):
    """Execute a query and return a DataFrame."""
    conn = get_connection()
    df = pd.read_sql_query(sql, conn, params=params or [])
    conn.close()
    return df


# ─── County-Level Overview ────────────────────────────────────────────────

def get_all_counties():
    """List all counties with metadata for the county picker."""
    return _q("""
        SELECT c.name, c.has_precinct_data, c.precinct_election_count,
               COUNT(DISTINCT p.id) as precinct_count,
               COUNT(DISTINCT r.id) as race_count,
               COUNT(DISTINCT r.election_id) as election_count
        FROM counties c
        LEFT JOIN precincts p ON p.county_id = c.id
        LEFT JOIN races r ON r.county_id = c.id
        GROUP BY c.id
        ORDER BY c.name
    """)


def get_county_overview(county):
    """High-level stats for a single county."""
    conn = get_connection()
    c = conn.cursor()

    county_row = c.execute("SELECT id, has_precinct_data FROM counties WHERE name = ?", (county,)).fetchone()
    if not county_row:
        conn.close()
        return None
    cid = county_row[0]

    elections = pd.read_sql_query(
        """SELECT e.election_name, e.election_date, e.election_type,
                  COUNT(DISTINCT r.id) as race_count,
                  SUM(res.votes) as total_votes,
                  MAX(res.data_level) as data_level
           FROM elections e
           JOIN races r ON r.election_id = e.id AND r.county_id = ?
           JOIN results res ON res.race_id = r.id
           GROUP BY e.id
           ORDER BY e.election_date DESC""",
        conn, params=[cid],
    )

    precinct_count = c.execute(
        "SELECT COUNT(*) FROM precincts WHERE county_id = ?", (cid,)
    ).fetchone()[0]

    total_races = c.execute(
        "SELECT COUNT(*) FROM races WHERE county_id = ?", (cid,)
    ).fetchone()[0]

    conn.close()

    return {
        "county": county,
        "has_precinct_data": bool(county_row[1]),
        "precinct_count": precinct_count,
        "total_races": total_races,
        "elections": elections,
    }


# ─── D Vote Share Analysis ────────────────────────────────────────────────

def get_dem_vote_share_by_election(county):
    """D vote share trend across elections for a county."""
    conn = get_connection()
    cid = conn.execute("SELECT id FROM counties WHERE name = ?", (county,)).fetchone()
    if not cid:
        conn.close()
        return pd.DataFrame()
    cid = cid[0]

    df = pd.read_sql_query("""
        SELECT e.election_name, e.election_date, e.election_type,
               r.race_name, r.race_level,
               SUM(CASE WHEN res.party = 'D' THEN res.votes ELSE 0 END) as d_votes,
               SUM(CASE WHEN res.party = 'R' THEN res.votes ELSE 0 END) as r_votes,
               SUM(res.votes) as total_votes
        FROM elections e
        JOIN races r ON r.election_id = e.id AND r.county_id = ?
        JOIN results res ON res.race_id = r.id
        WHERE res.data_level = (
            CASE WHEN (SELECT has_precinct_data FROM counties WHERE id = ?) = 1
                 THEN 'Precinct' ELSE 'Locality' END
        )
        GROUP BY e.id, r.id
        HAVING d_votes + r_votes > 0
    """, conn, params=[cid, cid])
    conn.close()

    if df.empty:
        return df

    df["d_share"] = (df["d_votes"] / (df["d_votes"] + df["r_votes"]) * 100).round(1)
    return df


def get_dem_share_summary(county):
    """Avg D share per election for chart display."""
    df = get_dem_vote_share_by_election(county)
    if df.empty:
        return df
    summary = df.groupby(["election_name", "election_date", "election_type"]).agg(
        avg_d_share=("d_share", "mean"),
        median_d_share=("d_share", "median"),
        contested_races=("d_share", "count"),
        total_d_votes=("d_votes", "sum"),
        total_r_votes=("r_votes", "sum"),
    ).reset_index().sort_values("election_date")

    summary["overall_d_share"] = (
        summary["total_d_votes"] / (summary["total_d_votes"] + summary["total_r_votes"]) * 100
    ).round(1)
    return summary


# ─── Competitive Races ────────────────────────────────────────────────────

def get_competitive_races(county, min_d_share=35, max_d_share=55):
    """Races where D candidate was competitive (35-55% range)."""
    df = get_dem_vote_share_by_election(county)
    if df.empty:
        return df
    competitive = df[(df["d_share"] >= min_d_share) & (df["d_share"] <= max_d_share)].copy()
    return competitive.sort_values("d_share", ascending=False)


def get_uncontested_races(county):
    """Races with only R candidates (no D challenger)."""
    conn = get_connection()
    cid = conn.execute("SELECT id FROM counties WHERE name = ?", (county,)).fetchone()
    if not cid:
        conn.close()
        return pd.DataFrame()
    cid = cid[0]

    df = pd.read_sql_query("""
        SELECT e.election_name, e.election_date, e.election_type,
               r.race_name, r.race_level,
               GROUP_CONCAT(DISTINCT res.party) as parties,
               SUM(res.votes) as total_votes
        FROM elections e
        JOIN races r ON r.election_id = e.id AND r.county_id = ?
        JOIN results res ON res.race_id = r.id
        GROUP BY r.id
        HAVING parties NOT LIKE '%D%' AND parties LIKE '%R%'
    """, conn, params=[cid])
    conn.close()
    return df.sort_values("election_date", ascending=False) if not df.empty else df


# ─── Precinct Analysis ────────────────────────────────────────────────────

def get_precinct_results(county, election_name=None):
    """Precinct-level results for a county (optionally filtered by election)."""
    conn = get_connection()
    cid = conn.execute("SELECT id FROM counties WHERE name = ?", (county,)).fetchone()
    if not cid:
        conn.close()
        return pd.DataFrame()
    cid = cid[0]

    where_clause = "AND e.election_name = ?" if election_name else ""
    params = [cid, cid, election_name] if election_name else [cid, cid]

    df = pd.read_sql_query(f"""
        SELECT e.election_name, e.election_date,
               p.precinct_name,
               r.race_name, r.race_level,
               res.candidate_name, res.party, res.votes
        FROM results res
        JOIN races r ON r.id = res.race_id AND r.county_id = ?
        JOIN elections e ON e.id = r.election_id
        LEFT JOIN precincts p ON p.id = res.precinct_id
        WHERE res.data_level = 'Precinct' AND res.precinct_id IS NOT NULL
          AND r.county_id = ? {where_clause}
    """, conn, params=params)
    conn.close()
    return df


def get_precinct_d_share(county, election_name=None):
    """D share by precinct across races for a given election."""
    df = get_precinct_results(county, election_name)
    if df.empty:
        return df

    # Aggregate D and R votes per precinct per race
    agg = df.groupby(["election_name", "precinct_name", "race_name"]).apply(
        lambda g: pd.Series({
            "d_votes": g.loc[g["party"] == "D", "votes"].sum(),
            "r_votes": g.loc[g["party"] == "R", "votes"].sum(),
            "total_votes": g["votes"].sum(),
        }),
        include_groups=False,
    ).reset_index()

    contested = agg[(agg["d_votes"] > 0) & (agg["r_votes"] > 0)].copy()
    if contested.empty:
        return contested

    contested["d_share"] = (contested["d_votes"] / (contested["d_votes"] + contested["r_votes"]) * 100).round(1)

    # Average D share across all races per precinct
    precinct_avg = contested.groupby(["election_name", "precinct_name"]).agg(
        avg_d_share=("d_share", "mean"),
        races_counted=("d_share", "count"),
        total_votes=("total_votes", "sum"),
    ).reset_index()

    return precinct_avg.sort_values("avg_d_share", ascending=False)


def get_precinct_shift(county, election1, election2):
    """D share shift between two elections by precinct."""
    df1 = get_precinct_d_share(county, election1)
    df2 = get_precinct_d_share(county, election2)

    if df1.empty or df2.empty:
        return pd.DataFrame()

    merged = df1.merge(
        df2,
        on="precinct_name",
        suffixes=("_old", "_new"),
    )
    merged["shift"] = (merged["avg_d_share_new"] - merged["avg_d_share_old"]).round(1)
    return merged.sort_values("shift", ascending=False)


# ─── 2026 Target Races ────────────────────────────────────────────────────

def get_target_races_2026(county):
    """Identify 2026 target races based on historical D performance."""
    df = get_dem_vote_share_by_election(county)
    if df.empty:
        return {"targets": pd.DataFrame(), "uncontested": pd.DataFrame()}

    # Filter to general elections only
    generals = df[df["election_type"] == "general"].copy()
    if generals.empty:
        return {"targets": pd.DataFrame(), "uncontested": pd.DataFrame()}

    # Average D share by race across elections
    race_avg = generals.groupby(["race_name", "race_level"]).agg(
        avg_d_share=("d_share", "mean"),
        max_d_share=("d_share", "max"),
        elections_contested=("d_share", "count"),
        latest_election=("election_date", "max"),
    ).reset_index()

    # Classify priority
    def classify(row):
        if row["avg_d_share"] >= 45:
            return "High"
        elif row["avg_d_share"] >= 38:
            return "Medium"
        else:
            return "Low"

    race_avg["priority"] = race_avg.apply(classify, axis=1)

    # Get uncontested races
    uncontested = get_uncontested_races(county)

    return {
        "targets": race_avg.sort_values("avg_d_share", ascending=False),
        "uncontested": uncontested,
    }


# ─── Statewide Comparison ────────────────────────────────────────────────

def get_statewide_summary():
    """Summary stats for all 92 counties for the statewide view."""
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT c.name as county,
               c.has_precinct_data,
               c.precinct_election_count,
               COUNT(DISTINCT p.id) as precinct_count,
               COUNT(DISTINCT r.election_id) as elections,
               COUNT(DISTINCT r.id) as races
        FROM counties c
        LEFT JOIN precincts p ON p.county_id = c.id
        LEFT JOIN races r ON r.county_id = c.id
        GROUP BY c.id
        ORDER BY c.name
    """, conn)
    conn.close()
    return df


def get_statewide_d_share():
    """D vote share by county across all elections (general only)."""
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT c.name as county,
               e.election_name, e.election_date,
               SUM(CASE WHEN res.party = 'D' THEN res.votes ELSE 0 END) as d_votes,
               SUM(CASE WHEN res.party = 'R' THEN res.votes ELSE 0 END) as r_votes,
               SUM(res.votes) as total_votes
        FROM counties c
        JOIN races r ON r.county_id = c.id
        JOIN elections e ON e.id = r.election_id AND e.election_type = 'general'
        JOIN results res ON res.race_id = r.id
        GROUP BY c.id, e.id
        HAVING d_votes + r_votes > 0
    """, conn)
    conn.close()

    if df.empty:
        return df

    df["d_share"] = (df["d_votes"] / (df["d_votes"] + df["r_votes"]) * 100).round(1)

    # Average across elections per county
    summary = df.groupby("county").agg(
        avg_d_share=("d_share", "mean"),
        latest_d_share=("d_share", "last"),
        elections=("election_name", "nunique"),
    ).reset_index().sort_values("avg_d_share", ascending=False)

    return summary


def get_statewide_uncontested_rate():
    """Uncontested rate (no D challenger) by county."""
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT c.name as county,
               r.id as race_id,
               GROUP_CONCAT(DISTINCT res.party) as parties
        FROM counties c
        JOIN races r ON r.county_id = c.id
        JOIN elections e ON e.id = r.election_id AND e.election_type = 'general'
        JOIN results res ON res.race_id = r.id
        GROUP BY r.id
    """, conn)
    conn.close()

    if df.empty:
        return pd.DataFrame()

    df["has_d"] = df["parties"].str.contains("D", na=False)
    df["has_r"] = df["parties"].str.contains("R", na=False)

    county_stats = df.groupby("county").agg(
        total_races=("race_id", "count"),
        d_contested=("has_d", "sum"),
    ).reset_index()

    county_stats["uncontested_pct"] = (
        (1 - county_stats["d_contested"] / county_stats["total_races"]) * 100
    ).round(1)

    return county_stats.sort_values("uncontested_pct", ascending=False)
