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
        WHERE res.precinct_id IS NOT NULL OR res.precinct_id IS NULL
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
    """
    vote_shares = get_dem_vote_share_by_election(db_path=db_path)
    if vote_shares.empty:
        return vote_shares

    competitive = vote_shares[abs(vote_shares["margin"]) <= min_margin].copy()
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
