"""
BCD Analysis Engine
Core analytics for Boone County Democratic election strategy.

Key analyses:
- Democratic vote share trends over time
- Precinct-level shift analysis
- Turnout analysis and opportunity identification
- Candidate viability scoring
- "Shifting blue" narrative support
"""

import pandas as pd
import numpy as np
import sqlite3
import os
from database import get_connection, DB_PATH


def get_dem_vote_share_by_election(race_level=None, db_path=None):
    """
    Calculate Democratic vote share for each election over time.
    This is the core "shifting blue" metric.
    """
    conn = get_connection(db_path)

    query = """
        SELECT
            e.election_date,
            e.election_type,
            e.election_name,
            r.race_name,
            r.race_level,
            c.name as candidate_name,
            c.party,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        WHERE r.race_level IN ('federal', 'state', 'county', 'local')
    """
    if race_level:
        query += f" AND r.race_level = '{race_level}'"
    query += " GROUP BY e.election_date, r.race_name, c.party ORDER BY e.election_date"

    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        return df

    # Calculate D vs R vote share per race per election
    results = []
    for (date, race), group in df.groupby(["election_date", "race_name"]):
        total = group["total_votes"].sum()
        dem_votes = group[group["party"] == "D"]["total_votes"].sum()
        rep_votes = group[group["party"] == "R"]["total_votes"].sum()

        results.append({
            "election_date": date,
            "race_name": race,
            "total_votes": total,
            "dem_votes": dem_votes,
            "rep_votes": rep_votes,
            "dem_share": round(dem_votes / total * 100, 2) if total > 0 else 0,
            "rep_share": round(rep_votes / total * 100, 2) if total > 0 else 0,
            "margin": round((dem_votes - rep_votes) / total * 100, 2) if total > 0 else 0,
        })

    return pd.DataFrame(results)


def get_precinct_shift(election_date_1, election_date_2, db_path=None):
    """
    Compare Democratic performance between two elections at the precinct level.
    Identifies which precincts are shifting blue/red.
    """
    conn = get_connection(db_path)

    query = """
        SELECT
            p.precinct_name,
            e.election_date,
            r.race_name,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        JOIN precincts p ON res.precinct_id = p.id
        WHERE e.election_date IN (?, ?)
        GROUP BY p.precinct_name, e.election_date, r.race_name, c.party
    """
    df = pd.read_sql_query(query, conn, params=[election_date_1, election_date_2])
    conn.close()

    if df.empty:
        return df

    # Calculate shift per precinct
    shifts = []
    for precinct in df["precinct_name"].unique():
        pct_data = df[df["precinct_name"] == precinct]

        for race in pct_data["race_name"].unique():
            race_data = pct_data[pct_data["race_name"] == race]

            e1 = race_data[race_data["election_date"] == election_date_1]
            e2 = race_data[race_data["election_date"] == election_date_2]

            e1_total = e1["votes"].sum()
            e2_total = e2["votes"].sum()
            e1_dem = e1[e1["party"] == "D"]["votes"].sum()
            e2_dem = e2[e2["party"] == "D"]["votes"].sum()

            e1_dem_share = (e1_dem / e1_total * 100) if e1_total > 0 else 0
            e2_dem_share = (e2_dem / e2_total * 100) if e2_total > 0 else 0

            shifts.append({
                "precinct": precinct,
                "race": race,
                f"dem_share_{election_date_1}": round(e1_dem_share, 2),
                f"dem_share_{election_date_2}": round(e2_dem_share, 2),
                "shift": round(e2_dem_share - e1_dem_share, 2),
                "direction": "BLUE" if e2_dem_share > e1_dem_share else "RED",
                f"turnout_{election_date_1}": e1_total,
                f"turnout_{election_date_2}": e2_total,
            })

    return pd.DataFrame(shifts).sort_values("shift", ascending=False)


def get_turnout_analysis(db_path=None):
    """
    Analyze turnout patterns to identify mobilization opportunities.
    Low-turnout precincts with Democratic lean = biggest opportunities.
    """
    conn = get_connection(db_path)

    query = """
        SELECT
            p.precinct_name,
            e.election_date,
            e.election_type,
            t.registered_voters,
            t.ballots_cast,
            t.turnout_percentage
        FROM turnout t
        JOIN elections e ON t.election_id = e.id
        JOIN precincts p ON t.precinct_id = p.id
        ORDER BY p.precinct_name, e.election_date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def get_competitive_races(min_margin=15, db_path=None):
    """
    Identify races where the margin was close enough to be competitive.
    Default: races decided by less than 15 points.
    Only includes races where both D and R received votes (actual contests).
    """
    vote_shares = get_dem_vote_share_by_election(db_path=db_path)
    if vote_shares.empty:
        return vote_shares

    # Filter to actual contested races (both D and R got votes)
    contested = vote_shares[(vote_shares["dem_votes"] > 0) & (vote_shares["rep_votes"] > 0)].copy()
    competitive = contested[abs(contested["margin"]) <= min_margin].copy()
    competitive = competitive.sort_values("margin", ascending=False)
    return competitive


def generate_summary_report(db_path=None):
    """
    Generate an overall summary of the database contents and key metrics.
    """
    conn = get_connection(db_path)

    report = {}

    # Basic counts
    for table in ["elections", "races", "candidates", "results", "precincts"]:
        cursor = conn.execute(f"SELECT COUNT(*) as cnt FROM {table}")
        report[f"total_{table}"] = cursor.fetchone()["cnt"]

    # Date range
    cursor = conn.execute("SELECT MIN(election_date) as earliest, MAX(election_date) as latest FROM elections")
    row = cursor.fetchone()
    report["earliest_election"] = row["earliest"]
    report["latest_election"] = row["latest"]

    # Party breakdown of candidates
    cursor = conn.execute("SELECT party, COUNT(*) as cnt FROM candidates GROUP BY party")
    report["candidates_by_party"] = {row["party"]: row["cnt"] for row in cursor.fetchall()}

    conn.close()

    print("=" * 60)
    print("BCD ELECTION DATA SUMMARY")
    print("=" * 60)
    print(f"Elections:   {report['total_elections']}")
    print(f"Races:       {report['total_races']}")
    print(f"Candidates:  {report['total_candidates']}")
    print(f"Results:     {report['total_results']}")
    print(f"Precincts:   {report['total_precincts']}")
    print(f"Date range:  {report['earliest_election']} to {report['latest_election']}")
    print(f"Candidates by party: {report['candidates_by_party']}")
    print("=" * 60)

    return report


def _get_precinct_dem_share_base(db_path=None):
    """
    Shared base query: D share per precinct per election.
    Used by precinct typology, turnout crossref, and heatmap views.
    Normalizes precinct names to UPPER() to avoid duplicates.
    """
    conn = get_connection(db_path)
    query = """
        SELECT
            UPPER(p.precinct_name) as precinct,
            e.election_date,
            e.election_type,
            SUM(CASE WHEN c.party = 'D' THEN res.votes ELSE 0 END) as dem_votes,
            SUM(CASE WHEN c.party = 'R' THEN res.votes ELSE 0 END) as rep_votes,
            SUM(CASE WHEN c.party IN ('D', 'R') THEN res.votes ELSE 0 END) as dr_total
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        JOIN precincts p ON res.precinct_id = p.id
        WHERE c.party IN ('D', 'R')
          AND res.precinct_id IS NOT NULL
        GROUP BY UPPER(p.precinct_name), e.election_date
        ORDER BY UPPER(p.precinct_name), e.election_date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        return df

    # Avoid division by zero; drop rows where no D or R votes exist
    df = df[df["dr_total"] > 0].copy()
    df["d_share"] = (df["dem_votes"] / df["dr_total"] * 100).round(2)
    df["d_margin"] = ((df["dem_votes"] - df["rep_votes"]) / df["dr_total"] * 100).round(2)
    return df


def get_precinct_heatmap_data(min_elections=3, db_path=None):
    """
    Build a matrix of D margin by precinct (rows) and election (columns).
    Perfect for visualizing the full trajectory of every precinct over time.
    """
    base = _get_precinct_dem_share_base(db_path)
    if base.empty:
        return base

    # Filter precincts with fewer than min_elections data points
    election_counts = base.groupby("precinct")["election_date"].nunique()
    valid_precincts = election_counts[election_counts >= min_elections].index
    base = base[base["precinct"].isin(valid_precincts)]

    if base.empty:
        return base

    # Pivot: rows = precinct, columns = election_date, values = d_margin
    heatmap = base.pivot_table(
        index="precinct",
        columns="election_date",
        values="d_margin",
        aggfunc="first"
    )

    # Sort precincts by average D margin (most D-leaning at top)
    heatmap["_avg_margin"] = heatmap.mean(axis=1)
    heatmap = heatmap.sort_values("_avg_margin", ascending=False)
    heatmap = heatmap.drop(columns=["_avg_margin"])

    return heatmap


def get_precinct_typology(recent_elections=6, top_pctile=75, mid_pctile=50,
                          trend_threshold=1.0, db_path=None):
    """
    Classify each precinct into strategic categories based on voting patterns.
    Uses percentile-based thresholds relative to the county's actual data,
    so it works whether the county is deep red, swing, or deep blue.

    Categories:
      - Best D: Top quartile of D share (above top_pctile)
      - Trending D: Below top quartile but trending up (slope >= trend_threshold)
      - Competitive: Middle range (between mid_pctile and top_pctile), or trending up from below
      - Lean R: Below mid_pctile, not trending strongly either way
      - Strong R: Bottom quartile with negative or flat trend

    Precincts with < 3 elections in the recent window get "Insufficient Data".
    """
    base = _get_precinct_dem_share_base(db_path)
    if base.empty:
        return base

    # Get the most recent N election dates
    all_dates = sorted(base["election_date"].unique())
    recent_dates = all_dates[-recent_elections:] if len(all_dates) >= recent_elections else all_dates
    recent = base[base["election_date"].isin(recent_dates)]

    # Compute per-precinct stats first to determine thresholds
    precinct_stats = []
    for precinct, group in recent.groupby("precinct"):
        group = group.sort_values("election_date")
        n_elections = len(group)
        avg_d = group["d_share"].mean()

        if n_elections >= 3:
            valid = group.dropna(subset=["d_share"])
            if len(valid) >= 3:
                x = np.arange(len(valid))
                y = valid["d_share"].values
                slope, _ = np.polyfit(x, y, 1)
            else:
                slope = 0.0
        else:
            slope = 0.0

        precinct_stats.append({
            "precinct": precinct,
            "avg_d_share": round(avg_d, 2),
            "d_trend": round(slope, 2),
            "elections_counted": n_elections,
            "latest_d_share": group["d_share"].iloc[-1],
            "min_d_share": group["d_share"].min(),
            "max_d_share": group["d_share"].max(),
        })

    result_df = pd.DataFrame(precinct_stats)
    if result_df.empty:
        return result_df

    # Calculate percentile thresholds from precincts with enough data
    valid = result_df[result_df["elections_counted"] >= 3]
    if valid.empty:
        result_df["category"] = "Insufficient Data"
        return result_df.sort_values("avg_d_share", ascending=False)

    top_thresh = np.percentile(valid["avg_d_share"], top_pctile)
    mid_thresh = np.percentile(valid["avg_d_share"], mid_pctile)
    bot_thresh = np.percentile(valid["avg_d_share"], 100 - top_pctile)

    def classify(row):
        if row["elections_counted"] < 3:
            return "Insufficient Data"
        avg = row["avg_d_share"]
        trend = row["d_trend"]
        if avg >= top_thresh:
            return "Best D"
        elif avg >= mid_thresh and trend >= trend_threshold:
            return "Trending D"
        elif avg >= mid_thresh:
            return "Competitive"
        elif avg >= bot_thresh:
            return "Lean R"
        else:
            return "Strong R"

    result_df["category"] = result_df.apply(classify, axis=1)

    # Store thresholds as DataFrame attribute for display
    result_df.attrs["top_threshold"] = round(top_thresh, 1)
    result_df.attrs["mid_threshold"] = round(mid_thresh, 1)
    result_df.attrs["bot_threshold"] = round(bot_thresh, 1)

    return result_df.sort_values("avg_d_share", ascending=False)


def get_turnout_vs_dem_share(election_date=None, turnout_cap=100.0, db_path=None):
    """
    Cross-reference turnout with Dem vote share per precinct.
    Identifies mobilization goldmine precincts (low turnout + high D share).
    """
    conn = get_connection(db_path)

    # Turnout data
    turnout_query = """
        SELECT
            UPPER(p.precinct_name) as precinct,
            AVG(t.turnout_percentage) as avg_turnout,
            AVG(t.registered_voters) as avg_registered
        FROM turnout t
        JOIN precincts p ON t.precinct_id = p.id
        JOIN elections e ON t.election_id = e.id
        WHERE t.turnout_percentage > 0 AND t.turnout_percentage <= ?
    """
    params = [turnout_cap]
    if election_date:
        turnout_query += " AND e.election_date = ?"
        params.append(election_date)
    turnout_query += " GROUP BY UPPER(p.precinct_name)"

    turnout_df = pd.read_sql_query(turnout_query, conn, params=params)
    conn.close()

    if turnout_df.empty:
        return turnout_df

    # D vote share data
    base = _get_precinct_dem_share_base(db_path)
    if base.empty:
        return pd.DataFrame()

    if election_date:
        base = base[base["election_date"] == election_date]

    dem_share_by_precinct = base.groupby("precinct").agg(
        avg_d_share=("d_share", "mean")
    ).reset_index()
    dem_share_by_precinct["avg_d_share"] = dem_share_by_precinct["avg_d_share"].round(2)

    # Merge
    merged = turnout_df.merge(dem_share_by_precinct, on="precinct", how="inner")
    if merged.empty:
        return merged

    merged["avg_turnout"] = merged["avg_turnout"].round(2)
    merged["avg_registered"] = merged["avg_registered"].round(0)

    # Assign quadrants based on medians
    med_turnout = merged["avg_turnout"].median()
    med_d_share = merged["avg_d_share"].median()

    def assign_quadrant(row):
        low_turnout = row["avg_turnout"] < med_turnout
        high_d = row["avg_d_share"] >= med_d_share
        if low_turnout and high_d:
            return "Mobilization Goldmine"
        elif not low_turnout and high_d:
            return "D Stronghold"
        elif not low_turnout and not high_d:
            return "R Stronghold"
        else:
            return "Low Priority"

    merged["quadrant"] = merged.apply(assign_quadrant, axis=1)

    # Potential votes gained for goldmine precincts
    merged["potential_votes_gained"] = 0.0
    goldmine_mask = merged["quadrant"] == "Mobilization Goldmine"
    merged.loc[goldmine_mask, "potential_votes_gained"] = (
        merged.loc[goldmine_mask, "avg_registered"]
        * (med_turnout - merged.loc[goldmine_mask, "avg_turnout"]) / 100
        * merged.loc[goldmine_mask, "avg_d_share"] / 100
    ).round(1)

    # Store medians for chart reference lines
    merged.attrs["median_turnout"] = med_turnout
    merged.attrs["median_d_share"] = med_d_share

    return merged


def get_downballot_dropoff(election_date=None, db_path=None):
    """
    Compare Dem performance across race levels (federal -> state -> county -> local).
    Only for 2016+ general elections where race_level labels exist.
    Returns (detail_df, summary_df).
    """
    conn = get_connection(db_path)

    query = """
        SELECT
            e.election_date,
            e.election_name,
            r.race_level,
            c.party,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        WHERE r.race_level IN ('federal', 'state', 'county', 'local')
          AND c.party IN ('D', 'R')
          AND e.election_type = 'general'
          AND e.election_date >= '2016-01-01'
    """
    params = []
    if election_date:
        query += " AND e.election_date = ?"
        params.append(election_date)

    query += " GROUP BY e.election_date, r.race_level, c.party ORDER BY e.election_date, r.race_level"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Calculate D share per (election, race_level)
    detail_rows = []
    for (date, level), group in df.groupby(["election_date", "race_level"]):
        d_votes = group[group["party"] == "D"]["total_votes"].sum()
        r_votes = group[group["party"] == "R"]["total_votes"].sum()
        total = d_votes + r_votes
        d_share = round(d_votes / total * 100, 2) if total > 0 else None

        ename = group["election_name"].iloc[0] if "election_name" in group.columns else ""

        detail_rows.append({
            "election_date": date,
            "election_name": ename,
            "race_level": level,
            "d_votes": d_votes,
            "r_votes": r_votes,
            "total_votes": total,
            "d_share": d_share,
        })

    detail_df = pd.DataFrame(detail_rows)

    # Build summary with drop-off calculations
    level_order = ["federal", "state", "county", "local"]
    summary_rows = []
    for date, group in detail_df.groupby("election_date"):
        row = {"election_date": date}
        shares = {}
        for level in level_order:
            level_data = group[group["race_level"] == level]
            shares[level] = level_data["d_share"].iloc[0] if not level_data.empty else None
            row[f"{level}_d_share"] = shares[level]

        # Calculate drop-off from federal
        fed = shares.get("federal")
        if fed is not None:
            for level in ["state", "county", "local"]:
                if shares.get(level) is not None:
                    row[f"fed_to_{level}_dropoff"] = round(fed - shares[level], 2)
                else:
                    row[f"fed_to_{level}_dropoff"] = None
        else:
            for level in ["state", "county", "local"]:
                row[f"fed_to_{level}_dropoff"] = None

        summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)
    return detail_df, summary_df


def get_straight_ticket_analysis(db_path=None):
    """
    Analyze straight-party voting patterns over time and by precinct.
    Returns (precinct_detail_df, trend_summary_df).
    """
    conn = get_connection(db_path)

    # Straight party votes by precinct and election
    query = """
        SELECT
            UPPER(p.precinct_name) as precinct,
            e.election_date,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        JOIN precincts p ON res.precinct_id = p.id
        WHERE r.race_name = 'Straight Party'
          AND res.precinct_id IS NOT NULL
          AND c.party IN ('D', 'R')
        GROUP BY UPPER(p.precinct_name), e.election_date, c.party
        ORDER BY UPPER(p.precinct_name), e.election_date
    """
    df = pd.read_sql_query(query, conn)

    # Turnout data for context (straight as % of ballots)
    turnout_query = """
        SELECT
            UPPER(p.precinct_name) as precinct,
            e.election_date,
            SUM(t.ballots_cast) as ballots_cast
        FROM turnout t
        JOIN elections e ON t.election_id = e.id
        JOIN precincts p ON t.precinct_id = p.id
        GROUP BY UPPER(p.precinct_name), e.election_date
    """
    turnout_df = pd.read_sql_query(turnout_query, conn)
    conn.close()

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Build precinct-level detail
    precinct_rows = []
    for (precinct, date), group in df.groupby(["precinct", "election_date"]):
        d_votes = group[group["party"] == "D"]["votes"].sum()
        r_votes = group[group["party"] == "R"]["votes"].sum()
        total = d_votes + r_votes

        # Get ballots cast for this precinct/election
        turnout_match = turnout_df[
            (turnout_df["precinct"] == precinct) & (turnout_df["election_date"] == date)
        ]
        ballots = turnout_match["ballots_cast"].sum() if not turnout_match.empty else None

        precinct_rows.append({
            "precinct": precinct,
            "election_date": date,
            "d_straight": d_votes,
            "r_straight": r_votes,
            "total_straight": total,
            "d_straight_pct": round(d_votes / total * 100, 2) if total > 0 else None,
            "ballots_cast": ballots,
            "straight_pct_of_ballots": round(total / ballots * 100, 2) if ballots and ballots > 0 else None,
        })

    precinct_detail = pd.DataFrame(precinct_rows)

    # Build county-wide trend summary
    trend_rows = []
    for date, group in precinct_detail.groupby("election_date"):
        total_d = group["d_straight"].sum()
        total_r = group["r_straight"].sum()
        total_straight = group["total_straight"].sum()
        total_ballots = group["ballots_cast"].sum() if group["ballots_cast"].notna().any() else None

        trend_rows.append({
            "election_date": date,
            "total_d_straight": total_d,
            "total_r_straight": total_r,
            "total_straight": total_straight,
            "d_straight_pct": round(total_d / total_straight * 100, 2) if total_straight > 0 else None,
            "total_ballots": total_ballots,
            "straight_pct_of_ballots": round(total_straight / total_ballots * 100, 2) if total_ballots and total_ballots > 0 else None,
        })

    trend_summary = pd.DataFrame(trend_rows)
    return precinct_detail, trend_summary


def get_precinct_volatility(min_elections=4, db_path=None):
    """
    Measures average election-to-election D share swing per precinct.
    High volatility = persuadable voters who change behavior.
    """
    base = _get_precinct_dem_share_base(db_path)
    if base.empty:
        return base

    results = []
    for precinct, group in base.sort_values("election_date").groupby("precinct"):
        if len(group) < min_elections:
            continue
        shares = group["d_share"].values
        swings = np.abs(np.diff(shares))
        results.append({
            "precinct": precinct,
            "volatility": round(float(np.mean(swings)), 2),
            "max_swing": round(float(np.max(swings)), 2),
            "elections_counted": len(group),
            "avg_d_share": round(float(group["d_share"].mean()), 2),
            "latest_d_share": round(float(shares[-1]), 2),
        })

    return pd.DataFrame(results).sort_values("volatility", ascending=False) if results else pd.DataFrame()


def get_precinct_pvi(db_path=None):
    """
    Local Cook PVI: each precinct's average presidential D share
    compared to the county average, expressed as D+X or R+X.
    Uses general election presidential races only.
    """
    conn = get_connection(db_path)
    query = """
        SELECT
            UPPER(p.precinct_name) as precinct,
            e.election_date,
            SUM(CASE WHEN c.party = 'D' THEN res.votes ELSE 0 END) as dem_votes,
            SUM(CASE WHEN c.party = 'R' THEN res.votes ELSE 0 END) as rep_votes,
            SUM(CASE WHEN c.party IN ('D', 'R') THEN res.votes ELSE 0 END) as dr_total
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        JOIN precincts p ON res.precinct_id = p.id
        WHERE r.race_name LIKE '%President%'
          AND c.party IN ('D', 'R')
          AND e.election_type = 'general'
          AND res.precinct_id IS NOT NULL
        GROUP BY UPPER(p.precinct_name), e.election_date
        HAVING dr_total > 0
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        return df

    df["d_share"] = (df["dem_votes"] / df["dr_total"] * 100).round(2)

    # County average D share per presidential election
    county_avg = df.groupby("election_date")["d_share"].mean().reset_index()
    county_avg.columns = ["election_date", "county_d_share"]

    # Merge and compute deviation
    merged = df.merge(county_avg, on="election_date")
    merged["deviation"] = merged["d_share"] - merged["county_d_share"]

    # Average deviation across all presidential elections = PVI
    pvi = merged.groupby("precinct").agg(
        pvi=("deviation", "mean"),
        avg_d_share=("d_share", "mean"),
        elections_counted=("election_date", "nunique"),
    ).reset_index()

    pvi["pvi"] = pvi["pvi"].round(1)
    pvi["avg_d_share"] = pvi["avg_d_share"].round(2)
    pvi["pvi_label"] = pvi["pvi"].apply(
        lambda x: f"D+{abs(x):.1f}" if x >= 0 else f"R+{abs(x):.1f}"
    )

    return pvi.sort_values("pvi", ascending=False)


def get_surge_voter_analysis(db_path=None):
    """
    Track registration growth per precinct vs D share change.
    Identifies 'Growing + Bluing' precincts = long-term investments.
    """
    conn = get_connection(db_path)

    turnout_query = """
        SELECT
            UPPER(p.precinct_name) as precinct,
            e.election_date,
            MAX(t.registered_voters) as registered_voters
        FROM turnout t
        JOIN elections e ON t.election_id = e.id
        JOIN precincts p ON t.precinct_id = p.id
        WHERE t.precinct_id IS NOT NULL
          AND t.registered_voters > 0
        GROUP BY UPPER(p.precinct_name), e.election_date
        ORDER BY UPPER(p.precinct_name), e.election_date
    """
    turnout_df = pd.read_sql_query(turnout_query, conn)
    conn.close()

    if turnout_df.empty:
        return pd.DataFrame()

    # Get D share base data
    base = _get_precinct_dem_share_base(db_path)
    if base.empty:
        return pd.DataFrame()

    # Compute growth: earliest vs latest registered_voters
    growth_rows = []
    for precinct, group in turnout_df.sort_values("election_date").groupby("precinct"):
        if len(group) < 2:
            continue
        earliest_reg = group.iloc[0]["registered_voters"]
        latest_reg = group.iloc[-1]["registered_voters"]
        earliest_date = group.iloc[0]["election_date"]
        latest_date = group.iloc[-1]["election_date"]

        reg_growth_pct = round((latest_reg - earliest_reg) / earliest_reg * 100, 2) if earliest_reg > 0 else 0.0

        # Get D share change for same precinct
        pct_base = base[base["precinct"] == precinct].sort_values("election_date")
        if len(pct_base) < 2:
            d_share_change = 0.0
        else:
            d_share_change = round(float(pct_base.iloc[-1]["d_share"] - pct_base.iloc[0]["d_share"]), 2)

        growth_rows.append({
            "precinct": precinct,
            "earliest_registered": int(earliest_reg),
            "latest_registered": int(latest_reg),
            "reg_growth_pct": reg_growth_pct,
            "d_share_change": d_share_change,
            "earliest_date": earliest_date,
            "latest_date": latest_date,
        })

    result = pd.DataFrame(growth_rows)
    if result.empty:
        return result

    # Assign quadrants
    med_growth = result["reg_growth_pct"].median()
    med_d_change = result["d_share_change"].median()

    def assign_quadrant(row):
        growing = row["reg_growth_pct"] >= med_growth
        bluing = row["d_share_change"] >= med_d_change
        if growing and bluing:
            return "Growing + Bluing"
        elif growing and not bluing:
            return "Growing + Reddening"
        elif not growing and bluing:
            return "Stable + Bluing"
        else:
            return "Stable + Reddening"

    result["quadrant"] = result.apply(assign_quadrant, axis=1)
    result.attrs["median_growth"] = med_growth
    result.attrs["median_d_change"] = med_d_change

    return result


def get_uncontested_race_mapping(db_path=None):
    """
    Map contested vs uncontested races per election.
    For uncontested R races, estimate latent D support from
    contested races at the same level in the same election.
    Returns (summary_pivot, uncontested_r_detail).
    """
    conn = get_connection(db_path)

    # Identify contested vs uncontested races
    query = """
        SELECT
            r.id as race_id,
            r.race_name,
            r.race_level,
            e.election_date,
            e.election_type,
            GROUP_CONCAT(DISTINCT c.party) as parties
        FROM races r
        JOIN results res ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        WHERE r.race_level IN ('federal', 'state', 'county', 'local')
          AND e.election_type = 'general'
          AND c.party IN ('D', 'R')
        GROUP BY r.id
    """
    races_df = pd.read_sql_query(query, conn)

    if races_df.empty:
        conn.close()
        return pd.DataFrame(), pd.DataFrame()

    # Classify each race
    races_df["status"] = races_df["parties"].apply(
        lambda p: "Contested" if "D" in str(p) and "R" in str(p)
        else ("Uncontested D" if "D" in str(p) else "Uncontested R")
    )

    # Summary: contested vs uncontested by election year
    summary = races_df.groupby(["election_date", "status"]).size().reset_index(name="count")
    summary_pivot = summary.pivot_table(
        index="election_date", columns="status", values="count", fill_value=0
    ).reset_index()

    # For uncontested R races, estimate latent D support
    uncontested_r = races_df[races_df["status"] == "Uncontested R"].copy()

    if not uncontested_r.empty:
        # Get D share from contested races per election per race_level
        contested_share_query = """
            SELECT
                e.election_date,
                r.race_level,
                SUM(CASE WHEN c.party = 'D' THEN res.votes ELSE 0 END) as d_votes,
                SUM(CASE WHEN c.party IN ('D','R') THEN res.votes ELSE 0 END) as dr_total
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN candidates c ON res.candidate_id = c.id
            JOIN elections e ON r.election_id = e.id
            WHERE r.race_level IN ('federal', 'state', 'county', 'local')
              AND e.election_type = 'general'
              AND c.party IN ('D', 'R')
              AND r.id IN (
                  SELECT r2.id FROM races r2
                  JOIN results res2 ON res2.race_id = r2.id
                  JOIN candidates c2 ON res2.candidate_id = c2.id
                  WHERE c2.party IN ('D','R')
                  GROUP BY r2.id
                  HAVING COUNT(DISTINCT c2.party) = 2
              )
            GROUP BY e.election_date, r.race_level
            HAVING dr_total > 0
        """
        contested_shares = pd.read_sql_query(contested_share_query, conn)
        if not contested_shares.empty:
            contested_shares["baseline_d_share"] = (
                contested_shares["d_votes"] / contested_shares["dr_total"] * 100
            ).round(2)

            # Get total votes for uncontested R races
            unc_race_ids = ",".join(str(x) for x in uncontested_r["race_id"].tolist())
            unc_votes_query = f"""
                SELECT r.id as race_id, SUM(res.votes) as total_votes
                FROM results res
                JOIN races r ON res.race_id = r.id
                WHERE r.id IN ({unc_race_ids})
                GROUP BY r.id
            """
            unc_votes = pd.read_sql_query(unc_votes_query, conn)

            uncontested_r = uncontested_r.merge(unc_votes, on="race_id", how="left")
            uncontested_r = uncontested_r.merge(
                contested_shares[["election_date", "race_level", "baseline_d_share"]],
                on=["election_date", "race_level"],
                how="left"
            )
            uncontested_r["estimated_latent_d_votes"] = (
                uncontested_r["total_votes"].fillna(0) * uncontested_r["baseline_d_share"].fillna(0) / 100
            ).round(0)

    conn.close()
    return summary_pivot, uncontested_r.sort_values(
        "estimated_latent_d_votes", ascending=False
    ) if not uncontested_r.empty else (summary_pivot, pd.DataFrame())


def get_third_party_persuadability(db_path=None):
    """
    Track L/I/WTP third-party vote share per precinct in general elections.
    Flags 'flippable' precincts where third-party vote exceeds D-R margin.
    Returns (aggregate_df, detail_df).
    """
    conn = get_connection(db_path)
    query = """
        SELECT
            UPPER(p.precinct_name) as precinct,
            e.election_date,
            SUM(CASE WHEN c.party = 'D' THEN res.votes ELSE 0 END) as dem_votes,
            SUM(CASE WHEN c.party = 'R' THEN res.votes ELSE 0 END) as rep_votes,
            SUM(CASE WHEN c.party IN ('L', 'I', 'WTP')
                THEN res.votes ELSE 0 END) as third_party_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        JOIN precincts p ON res.precinct_id = p.id
        WHERE e.election_type = 'general'
          AND r.race_name <> 'Straight Party'
          AND r.race_level IN ('federal', 'state', 'county', 'local')
          AND res.precinct_id IS NOT NULL
        GROUP BY UPPER(p.precinct_name), e.election_date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = df[df["total_votes"] > 0].copy()
    df["third_party_pct"] = (df["third_party_votes"] / df["total_votes"] * 100).round(2)
    df["dr_margin"] = ((df["dem_votes"] - df["rep_votes"]) / df["total_votes"] * 100).round(2)
    df["margin_abs"] = df["dr_margin"].abs()
    df["flippable"] = df["third_party_pct"] > df["margin_abs"]

    # Aggregate across elections
    agg = df.groupby("precinct").agg(
        avg_third_party_pct=("third_party_pct", "mean"),
        avg_margin=("dr_margin", "mean"),
        flippable_elections=("flippable", "sum"),
        total_elections=("election_date", "nunique"),
    ).reset_index()

    agg["avg_third_party_pct"] = agg["avg_third_party_pct"].round(2)
    agg["avg_margin"] = agg["avg_margin"].round(2)
    agg = agg.sort_values("avg_third_party_pct", ascending=False)

    return agg, df


def get_rolloff_analysis(db_path=None):
    """
    Computes ballot rolloff: voters who cast a ballot but skip a race.
    rolloff = (ballots_cast - race_votes) / ballots_cast per precinct per election.
    Returns (avg_rolloff_df, heatmap_df).
    """
    conn = get_connection(db_path)

    turnout_query = """
        SELECT
            UPPER(p.precinct_name) as precinct,
            e.election_date,
            MAX(t.ballots_cast) as ballots_cast
        FROM turnout t
        JOIN elections e ON t.election_id = e.id
        JOIN precincts p ON t.precinct_id = p.id
        WHERE t.precinct_id IS NOT NULL
          AND t.ballots_cast > 0
        GROUP BY UPPER(p.precinct_name), e.election_date
    """
    turnout_df = pd.read_sql_query(turnout_query, conn)

    votes_query = """
        SELECT
            UPPER(p.precinct_name) as precinct,
            e.election_date,
            r.race_name,
            r.race_level,
            SUM(res.votes) as race_total_votes
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN precincts p ON res.precinct_id = p.id
        WHERE res.precinct_id IS NOT NULL
          AND r.race_name <> 'Straight Party'
          AND r.race_level IN ('federal', 'state', 'county', 'local')
        GROUP BY UPPER(p.precinct_name), e.election_date, r.id
    """
    votes_df = pd.read_sql_query(votes_query, conn)
    conn.close()

    if turnout_df.empty or votes_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Merge to get ballots_cast alongside race votes
    merged = votes_df.merge(turnout_df, on=["precinct", "election_date"], how="inner")

    # Compute rolloff per race, clamp at 0 (multi-vote races can exceed ballots_cast)
    merged["rolloff"] = (
        (merged["ballots_cast"] - merged["race_total_votes"])
        / merged["ballots_cast"] * 100
    ).round(2)
    merged["rolloff"] = merged["rolloff"].clip(lower=0)

    # Average rolloff per precinct per election
    avg_rolloff = merged.groupby(["precinct", "election_date"]).agg(
        avg_rolloff=("rolloff", "mean"),
        max_rolloff=("rolloff", "max"),
        races_counted=("race_name", "nunique"),
    ).reset_index()
    avg_rolloff["avg_rolloff"] = avg_rolloff["avg_rolloff"].round(2)

    # Pivot for heatmap
    heatmap = avg_rolloff.pivot_table(
        index="precinct",
        columns="election_date",
        values="avg_rolloff",
        aggfunc="first"
    )
    heatmap["_avg"] = heatmap.mean(axis=1)
    heatmap = heatmap.sort_values("_avg", ascending=False)
    heatmap = heatmap.drop(columns=["_avg"])

    return avg_rolloff, heatmap


def get_straight_ticket_geography(db_path=None):
    """
    Straight-ticket D votes as % of total D votes per precinct per election.
    Classifies: Brand-Dependent (>=40%), Mixed (20-40%), Candidate-Dependent (<20%).
    """
    conn = get_connection(db_path)

    query = """
        SELECT
            UPPER(p.precinct_name) as precinct,
            e.election_date,
            SUM(CASE WHEN r.race_name = 'Straight Party' AND c.party = 'D'
                THEN res.votes ELSE 0 END) as straight_d_votes,
            SUM(CASE WHEN c.party = 'D'
                THEN res.votes ELSE 0 END) as total_d_votes
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        JOIN precincts p ON res.precinct_id = p.id
        WHERE res.precinct_id IS NOT NULL
          AND e.election_type = 'general'
        GROUP BY UPPER(p.precinct_name), e.election_date
        HAVING total_d_votes > 0
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        return df

    df["straight_d_pct_of_total"] = (
        df["straight_d_votes"] / df["total_d_votes"] * 100
    ).round(2)

    df["dependency"] = df["straight_d_pct_of_total"].apply(
        lambda x: "Brand-Dependent" if x >= 40 else (
            "Mixed" if x >= 20 else "Candidate-Dependent"
        )
    )

    return df


def get_headline_kpis(db_path=None):
    """
    Generate headline KPIs with deltas comparing latest vs prior general election.
    Returns dict with D share, turnout, straight-ticket D%, contested race counts.
    """
    conn = get_connection(db_path)

    elections_query = """
        SELECT DISTINCT election_date FROM elections
        WHERE election_type = 'general'
        ORDER BY election_date DESC LIMIT 2
    """
    elections = pd.read_sql_query(elections_query, conn)

    if len(elections) < 2:
        conn.close()
        return {}

    latest = elections.iloc[0]["election_date"]
    prior = elections.iloc[1]["election_date"]

    # D vote share
    share_query = """
        SELECT
            e.election_date,
            SUM(CASE WHEN c.party = 'D' THEN res.votes ELSE 0 END) as dem_votes,
            SUM(CASE WHEN c.party IN ('D', 'R') THEN res.votes ELSE 0 END) as dr_total
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        WHERE e.election_date IN (?, ?)
          AND c.party IN ('D', 'R')
          AND r.race_level IN ('federal', 'state', 'county', 'local')
        GROUP BY e.election_date
    """
    share_df = pd.read_sql_query(share_query, conn, params=[latest, prior])

    # Turnout
    turnout_query = """
        SELECT
            e.election_date,
            AVG(t.turnout_percentage) as avg_turnout
        FROM turnout t
        JOIN elections e ON t.election_id = e.id
        WHERE e.election_date IN (?, ?)
          AND t.precinct_id IS NOT NULL
          AND t.turnout_percentage > 0 AND t.turnout_percentage <= 100
        GROUP BY e.election_date
    """
    turnout_df = pd.read_sql_query(turnout_query, conn, params=[latest, prior])

    # Straight-ticket D%
    straight_query = """
        SELECT
            e.election_date,
            SUM(CASE WHEN c.party = 'D' THEN res.votes ELSE 0 END) as d_straight,
            SUM(res.votes) as total_straight
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        WHERE r.race_name = 'Straight Party'
          AND c.party IN ('D', 'R')
          AND e.election_date IN (?, ?)
        GROUP BY e.election_date
    """
    straight_df = pd.read_sql_query(straight_query, conn, params=[latest, prior])

    # D contested races
    contested_query = """
        SELECT
            e.election_date,
            COUNT(DISTINCT r.id) as d_contested
        FROM races r
        JOIN results res ON res.race_id = r.id
        JOIN candidates c ON res.candidate_id = c.id
        JOIN elections e ON r.election_id = e.id
        WHERE c.party = 'D'
          AND e.election_date IN (?, ?)
          AND r.race_level IN ('federal', 'state', 'county', 'local')
        GROUP BY e.election_date
    """
    contested_df = pd.read_sql_query(contested_query, conn, params=[latest, prior])

    conn.close()

    # Build KPI dict
    kpis = {"latest_election": latest, "prior_election": prior}

    for _, row in share_df.iterrows():
        d_share = round(row["dem_votes"] / row["dr_total"] * 100, 1) if row["dr_total"] > 0 else 0
        if row["election_date"] == latest:
            kpis["d_share_latest"] = d_share
        else:
            kpis["d_share_prior"] = d_share
    kpis["d_share_delta"] = round(kpis.get("d_share_latest", 0) - kpis.get("d_share_prior", 0), 1)

    for _, row in turnout_df.iterrows():
        if row["election_date"] == latest:
            kpis["turnout_latest"] = round(row["avg_turnout"], 1)
        else:
            kpis["turnout_prior"] = round(row["avg_turnout"], 1)
    kpis["turnout_delta"] = round(kpis.get("turnout_latest", 0) - kpis.get("turnout_prior", 0), 1)

    for _, row in straight_df.iterrows():
        d_pct = round(row["d_straight"] / row["total_straight"] * 100, 1) if row["total_straight"] > 0 else 0
        if row["election_date"] == latest:
            kpis["straight_d_pct_latest"] = d_pct
        else:
            kpis["straight_d_pct_prior"] = d_pct
    kpis["straight_d_delta"] = round(
        kpis.get("straight_d_pct_latest", 0) - kpis.get("straight_d_pct_prior", 0), 1
    )

    for _, row in contested_df.iterrows():
        if row["election_date"] == latest:
            kpis["d_contested_latest"] = int(row["d_contested"])
        else:
            kpis["d_contested_prior"] = int(row["d_contested"])
    kpis["d_contested_delta"] = kpis.get("d_contested_latest", 0) - kpis.get("d_contested_prior", 0)

    return kpis


def get_top_opportunities(db_path=None):
    """
    Auto-generate top 3 strategic opportunities by synthesizing other analyses.
    Returns list of dicts: [{"title": str, "detail": str, "source": str}, ...]
    """
    opportunities = []

    # 1: Best mobilization goldmine precinct
    try:
        turnout_dem = get_turnout_vs_dem_share(db_path=db_path)
        if not turnout_dem.empty:
            goldmines = turnout_dem[turnout_dem["quadrant"] == "Mobilization Goldmine"]
            if not goldmines.empty:
                top = goldmines.sort_values("potential_votes_gained", ascending=False).iloc[0]
                opportunities.append({
                    "title": f"Mobilize {top['precinct']}",
                    "detail": (
                        f"Turnout {top['avg_turnout']:.0f}% with {top['avg_d_share']:.0f}% D share. "
                        f"Potential gain: ~{top['potential_votes_gained']:.0f} votes."
                    ),
                    "source": "Turnout Opportunities",
                })
    except Exception:
        pass

    # 2: Highest volatility D-leaning precinct
    try:
        volatility = get_precinct_volatility(db_path=db_path)
        if not volatility.empty:
            persuadable = volatility[volatility["avg_d_share"] >= 35].head(1)
            if not persuadable.empty:
                top = persuadable.iloc[0]
                opportunities.append({
                    "title": f"Persuade in {top['precinct']}",
                    "detail": (
                        f"Volatility: {top['volatility']:.1f} pp swing/election. "
                        f"Currently {top['latest_d_share']:.0f}% D -- winnable with engagement."
                    ),
                    "source": "Precinct Volatility",
                })
    except Exception:
        pass

    # 3: Best uncontested R seat to contest
    try:
        _, uncontested_detail = get_uncontested_race_mapping(db_path=db_path)
        if not uncontested_detail.empty and "estimated_latent_d_votes" in uncontested_detail.columns:
            latest_unc = uncontested_detail[
                uncontested_detail["election_date"] == uncontested_detail["election_date"].max()
            ]
            if not latest_unc.empty:
                top = latest_unc.sort_values("estimated_latent_d_votes", ascending=False).iloc[0]
                if pd.notna(top.get("estimated_latent_d_votes", None)):
                    opportunities.append({
                        "title": f"Contest {top['race_name']}",
                        "detail": (
                            f"Uncontested R in {top['election_date']}. "
                            f"Est. latent D support: ~{top['estimated_latent_d_votes']:.0f} votes."
                        ),
                        "source": "Uncontested Race Mapping",
                    })
    except Exception:
        pass

    # 4 (fallback): Fastest-growing D-trending precinct
    if len(opportunities) < 3:
        try:
            surge = get_surge_voter_analysis(db_path=db_path)
            if not surge.empty:
                growing_blue = surge[surge["quadrant"] == "Growing + Bluing"]
                if not growing_blue.empty:
                    top = growing_blue.sort_values("reg_growth_pct", ascending=False).iloc[0]
                    opportunities.append({
                        "title": f"Invest in {top['precinct']}",
                        "detail": (
                            f"Registration up {top['reg_growth_pct']:.0f}% and D share "
                            f"shifted +{top['d_share_change']:.1f} pp. A growing base."
                        ),
                        "source": "Growth Analysis",
                    })
        except Exception:
            pass

    return opportunities[:3]


def get_election_overview(db_path=None):
    """
    Get a summary overview of all elections for the Data Explorer tab.
    Returns one row per election with counts and quality scores.
    """
    conn = get_connection(db_path)

    query = """
        SELECT
            e.id as election_id,
            e.election_date,
            e.election_type,
            e.election_name,
            COUNT(DISTINCT r.id) as race_count,
            COUNT(DISTINCT res.id) as result_count,
            COUNT(DISTINCT res.precinct_id) as precinct_count,
            COALESCE(t_sub.turnout_precincts, 0) as turnout_precincts,
            COALESCE(dq.overall_confidence, 'not assessed') as confidence_level,
            COALESCE(dq.confidence_score, 0.0) as confidence_score,
            dq.source_type,
            dq.cross_validated,
            dq.race_names_clean,
            dq.turnout_consistent,
            dq.precinct_count_match,
            dq.notes as quality_notes
        FROM elections e
        LEFT JOIN races r ON r.election_id = e.id
        LEFT JOIN results res ON res.race_id = r.id
        LEFT JOIN data_quality dq ON dq.election_id = e.id
        LEFT JOIN (
            SELECT election_id, COUNT(DISTINCT precinct_id) as turnout_precincts
            FROM turnout
            GROUP BY election_id
        ) t_sub ON t_sub.election_id = e.id
        GROUP BY e.id
        ORDER BY e.election_date
    """

    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def export_analysis_to_excel(output_path=None, db_path=None):
    """
    Export all key analyses to a single Excel workbook.
    Perfect for sharing with non-technical party members.
    """
    if output_path is None:
        output_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "exports",
            f"bcd_analysis_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx"
        )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Dem vote share trends
        vote_shares = get_dem_vote_share_by_election(db_path=db_path)
        if not vote_shares.empty:
            vote_shares.to_excel(writer, sheet_name="Dem Vote Share Trends", index=False)

        # Competitive races
        competitive = get_competitive_races(db_path=db_path)
        if not competitive.empty:
            competitive.to_excel(writer, sheet_name="Competitive Races", index=False)

        # Turnout
        turnout = get_turnout_analysis(db_path=db_path)
        if not turnout.empty:
            turnout.to_excel(writer, sheet_name="Turnout Analysis", index=False)

    print(f"Analysis exported to: {output_path}")
    return output_path


if __name__ == "__main__":
    generate_summary_report()
