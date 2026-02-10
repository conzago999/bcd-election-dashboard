"""
BCD Election Dashboard
Streamlit-based interactive dashboard for Boone County election data.
Run with: streamlit run dashboards/app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))
from database import get_connection, DB_PATH
from analysis import (
    get_dem_vote_share_by_election,
    get_precinct_shift,
    get_competitive_races,
    get_turnout_analysis,
    generate_summary_report,
    get_precinct_typology,
    get_turnout_vs_dem_share,
    get_downballot_dropoff,
    get_straight_ticket_analysis,
    get_precinct_heatmap_data,
)

st.set_page_config(
    page_title="BCD Election Data",
    page_icon="\U0001f5f3\ufe0f",
    layout="wide",
)

st.title("Boone County Democrats - Election Data Dashboard")
st.markdown("*Data-driven strategy for Democratic success in Boone County, Indiana*")

# Check if database exists and has data
if not os.path.exists(DB_PATH):
    st.warning("Database not initialized yet. Run `python src/database.py` first.")
    st.stop()

conn = get_connection()
election_count = conn.execute("SELECT COUNT(*) as cnt FROM elections").fetchone()["cnt"]
conn.close()

if election_count == 0:
    st.info("No election data loaded yet. Import data using the tools in src/")
    st.markdown("""
    ### Getting Started
    1. Place your Excel/CSV files in `data/raw_excel/`
    2. Place your PDF files in `data/raw_pdfs/`
    3. Run: `python src/import_excel.py <your_file> --preview`
    4. Then import: `python src/import_excel.py <your_file>`
    5. Reload this dashboard
    """)
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")
conn = get_connection()
elections = pd.read_sql_query("SELECT DISTINCT election_date, election_name FROM elections ORDER BY election_date", conn)
conn.close()

# Category color maps used across multiple tabs
CATEGORY_COLORS = {
    "Best D": "#1a5276",
    "Trending D": "#5dade2",
    "Competitive": "#f4d03f",
    "Lean R": "#e74c3c",
    "Strong R": "#922b21",
    "Insufficient Data": "#bdc3c7",
}

QUADRANT_COLORS = {
    "Mobilization Goldmine": "#2ecc71",
    "D Stronghold": "#3498db",
    "R Stronghold": "#e74c3c",
    "Low Priority": "#95a5a6",
}

LEVEL_COLORS = {
    "federal": "#1a5276",
    "state": "#2980b9",
    "county": "#5dade2",
    "local": "#aed6f1",
}

# ============================================================
# TABS
# ============================================================
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "Overview",
    "Blue Shift Trends",
    "Precinct Typology",
    "Precinct Heatmap",
    "Precinct Shift",
    "Turnout Opportunities",
    "Downballot Drop-off",
    "Straight-Ticket Trends",
    "Competitive Races",
])

# ============================================================
# TAB 1: Overview
# ============================================================
with tab1:
    st.header("Data Overview")
    report = generate_summary_report()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Elections", report["total_elections"])
    col2.metric("Races", report["total_races"])
    col3.metric("Candidates", report["total_candidates"])
    col4.metric("Precincts", report["total_precincts"])

    st.markdown(f"**Data spans:** {report['earliest_election']} to {report['latest_election']}")

# ============================================================
# TAB 2: Blue Shift Trends
# ============================================================
with tab2:
    st.header("Democratic Vote Share Over Time")
    st.markdown("*Tracking the blue shift in Boone County*")

    vote_shares = get_dem_vote_share_by_election()

    if not vote_shares.empty:
        # Overall trend
        avg_by_election = vote_shares.groupby("election_date").agg({
            "dem_share": "mean",
            "rep_share": "mean",
            "margin": "mean"
        }).reset_index()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=avg_by_election["election_date"],
            y=avg_by_election["dem_share"],
            name="Democratic %",
            line=dict(color="blue", width=3),
            mode="lines+markers"
        ))
        fig.add_trace(go.Scatter(
            x=avg_by_election["election_date"],
            y=avg_by_election["rep_share"],
            name="Republican %",
            line=dict(color="red", width=3),
            mode="lines+markers"
        ))
        fig.update_layout(
            title="Average Vote Share Across All Races",
            yaxis_title="Vote Share %",
            xaxis_title="Election Date",
            hovermode="x unified"
        )
        st.plotly_chart(fig, use_container_width=True)

        # Margin trend
        fig2 = px.bar(
            avg_by_election,
            x="election_date",
            y="margin",
            color="margin",
            color_continuous_scale=["red", "white", "blue"],
            color_continuous_midpoint=0,
            title="D-R Margin Over Time (positive = Democratic advantage)"
        )
        st.plotly_chart(fig2, use_container_width=True)

        # Raw data
        with st.expander("View raw vote share data"):
            st.dataframe(vote_shares)
    else:
        st.info("No vote share data available yet.")

# ============================================================
# TAB 3: Precinct Typology
# ============================================================
with tab3:
    st.header("Precinct Targeting Typology")
    st.markdown("*Classifies each precinct for strategic resource allocation*")

    col1, col2, col3 = st.columns(3)
    with col1:
        typ_recent = st.slider("Recent elections to consider", 3, 15, 8, key="typ_recent")
    with col2:
        typ_top_pct = st.slider("Top tier percentile", 60, 90, 75, key="typ_top_pct",
                                help="Precincts above this percentile of D share = 'Best D'")
    with col3:
        typ_trend = st.slider("Trend sensitivity (pp/election)", 0.5, 5.0, 1.0, step=0.5, key="typ_trend")

    typology = get_precinct_typology(
        recent_elections=typ_recent,
        top_pctile=typ_top_pct,
        trend_threshold=typ_trend,
    )

    if not typology.empty:
        # Summary donut + bar chart side by side
        col_left, col_right = st.columns([1, 2])

        with col_left:
            cat_counts = typology["category"].value_counts().reset_index()
            cat_counts.columns = ["category", "count"]
            fig_pie = px.pie(
                cat_counts,
                values="count",
                names="category",
                color="category",
                color_discrete_map=CATEGORY_COLORS,
                hole=0.4,
                title="Precinct Distribution",
            )
            fig_pie.update_layout(height=400)
            st.plotly_chart(fig_pie, use_container_width=True)

        with col_right:
            fig_bar = px.bar(
                typology.sort_values("avg_d_share"),
                x="avg_d_share",
                y="precinct",
                color="category",
                color_discrete_map=CATEGORY_COLORS,
                orientation="h",
                title="Precincts by Avg Democratic Vote Share",
                hover_data=["d_trend", "latest_d_share", "elections_counted"],
            )
            fig_bar.add_vline(x=50, line_dash="dash", line_color="gray", opacity=0.5)
            fig_bar.update_layout(height=max(500, len(typology) * 16))
            st.plotly_chart(fig_bar, use_container_width=True)

        # Show computed thresholds
        top_t = typology.attrs.get("top_threshold", "N/A")
        mid_t = typology.attrs.get("mid_threshold", "N/A")
        st.caption(f"Thresholds (from data): Best D \u2265 {top_t}% D share | "
                   f"Competitive/Trending D \u2265 {mid_t}% | Below = Lean R / Strong R")

        with st.expander("View typology data"):
            st.dataframe(typology, hide_index=True)
    else:
        st.info("No precinct data available for typology analysis.")

# ============================================================
# TAB 4: Precinct Heatmap
# ============================================================
with tab4:
    st.header("Precinct Heatmap Over Time")
    st.markdown("*D margin by precinct across every election (blue = D advantage, red = R advantage)*")

    col1, col2 = st.columns(2)
    with col1:
        heatmap_min_elections = st.slider("Min elections for a precinct to appear", 1, 15, 3, key="hm_min")
    with col2:
        heatmap_sort = st.radio("Sort precincts by", ["Avg D margin", "Latest D margin", "Alphabetical"], key="hm_sort")

    heatmap = get_precinct_heatmap_data(min_elections=heatmap_min_elections)

    if not heatmap.empty:
        # Re-sort based on user selection
        if heatmap_sort == "Latest D margin":
            last_col = heatmap.columns[-1]
            heatmap = heatmap.sort_values(last_col, ascending=False)
        elif heatmap_sort == "Alphabetical":
            heatmap = heatmap.sort_index(ascending=True)
        # Default "Avg D margin" is already sorted by get_precinct_heatmap_data

        fig = px.imshow(
            heatmap,
            color_continuous_scale=["#922b21", "#e74c3c", "#f5b7b1", "white", "#aed6f1", "#3498db", "#1a5276"],
            color_continuous_midpoint=0,
            zmin=-60,
            zmax=60,
            title="D Margin by Precinct and Election",
            labels=dict(x="Election Date", y="Precinct", color="D Margin %"),
            aspect="auto",
        )
        fig.update_layout(
            height=max(600, len(heatmap) * 16),
            xaxis=dict(tickangle=45),
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("View heatmap data"):
            st.dataframe(heatmap.style.format(precision=1, na_rep="-"))
    else:
        st.info("No data available for heatmap.")

# ============================================================
# TAB 5: Precinct Shift (existing)
# ============================================================
with tab5:
    st.header("Precinct-Level Shift Analysis")
    st.markdown("*Compare Democratic performance between two elections*")

    if len(elections) >= 2:
        col1, col2 = st.columns(2)
        with col1:
            date1 = st.selectbox("Earlier election", elections["election_date"].tolist(), index=0)
        with col2:
            date2 = st.selectbox("Later election", elections["election_date"].tolist(),
                                index=min(1, len(elections)-1))

        if date1 != date2:
            shifts = get_precinct_shift(date1, date2)
            if not shifts.empty:
                fig = px.bar(
                    shifts.sort_values("shift"),
                    x="shift",
                    y="precinct",
                    orientation="h",
                    color="shift",
                    color_continuous_scale=["red", "white", "blue"],
                    color_continuous_midpoint=0,
                    title=f"Precinct Shift: {date1} \u2192 {date2}"
                )
                fig.update_layout(height=max(400, len(shifts) * 15))
                st.plotly_chart(fig, use_container_width=True)

                with st.expander("View shift data"):
                    st.dataframe(shifts)
    else:
        st.info("Need at least 2 elections to compare precinct shifts.")

# ============================================================
# TAB 6: Turnout Opportunities
# ============================================================
with tab6:
    st.header("Turnout Opportunity Analysis")
    st.markdown("*Find precincts where low turnout + high D share = mobilization goldmine*")

    col1, col2 = st.columns(2)
    with col1:
        turnout_election = st.selectbox(
            "Election",
            ["All elections (average)"] + elections["election_date"].tolist(),
            key="turnout_election"
        )
    with col2:
        turnout_cap = st.slider("Turnout cap (%)", 80, 120, 100, key="turnout_cap",
                                help="Exclude outlier turnout records above this threshold")

    election_filter = None if turnout_election == "All elections (average)" else turnout_election

    turnout_dem = get_turnout_vs_dem_share(
        election_date=election_filter,
        turnout_cap=float(turnout_cap),
    )

    if not turnout_dem.empty:
        # Get medians for reference lines
        med_turnout = turnout_dem.attrs.get("median_turnout", turnout_dem["avg_turnout"].median())
        med_d_share = turnout_dem.attrs.get("median_d_share", turnout_dem["avg_d_share"].median())

        fig = px.scatter(
            turnout_dem,
            x="avg_turnout",
            y="avg_d_share",
            color="quadrant",
            color_discrete_map=QUADRANT_COLORS,
            size="avg_registered",
            hover_data=["precinct", "potential_votes_gained"],
            title="Turnout vs. Dem Vote Share by Precinct",
        )
        fig.add_hline(y=med_d_share, line_dash="dash", line_color="gray", opacity=0.5,
                      annotation_text=f"Median D Share: {med_d_share:.1f}%")
        fig.add_vline(x=med_turnout, line_dash="dash", line_color="gray", opacity=0.5,
                      annotation_text=f"Median Turnout: {med_turnout:.1f}%")
        fig.update_layout(
            xaxis_title="Avg Turnout %",
            yaxis_title="Avg D Vote Share %",
            height=600,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Goldmine table
        goldmines = turnout_dem[turnout_dem["quadrant"] == "Mobilization Goldmine"].sort_values(
            "potential_votes_gained", ascending=False
        )
        if not goldmines.empty:
            st.subheader(f"Mobilization Goldmine Precincts ({len(goldmines)})")
            st.caption("These precincts have above-median D vote share but below-median turnout. "
                       "Potential votes gained estimates additional D votes from raising turnout to the median.")
            st.dataframe(
                goldmines[["precinct", "avg_turnout", "avg_d_share", "avg_registered", "potential_votes_gained"]],
                hide_index=True,
            )
        else:
            st.info("No mobilization goldmine precincts found with current filters.")

        with st.expander("View all precinct data"):
            st.dataframe(turnout_dem, hide_index=True)

        st.caption("Turnout records exceeding the cap have been excluded as data quality outliers.")
    else:
        st.info("No turnout + vote share data available.")

# ============================================================
# TAB 7: Downballot Drop-off
# ============================================================
with tab7:
    st.header("Downballot Drop-off Analysis")
    st.markdown("*Where does Dem support erode going from federal to local races?*")

    st.info("Race-level labels (federal/state/county/local) are only available for 2016+ general elections. "
            "Earlier elections have all races categorized as 'other'.")

    # Get available general elections from 2016+
    conn = get_connection()
    general_elections = pd.read_sql_query(
        "SELECT DISTINCT election_date, election_name FROM elections "
        "WHERE election_type = 'general' AND election_date >= '2016-01-01' ORDER BY election_date",
        conn
    )
    conn.close()

    if not general_elections.empty:
        db_election = st.selectbox(
            "Election",
            ["All 2016+ general elections"] + general_elections["election_date"].tolist(),
            key="db_election"
        )

        election_filter = None if db_election == "All 2016+ general elections" else db_election
        detail_df, summary_df = get_downballot_dropoff(election_date=election_filter)

        if not detail_df.empty:
            # Grouped bar chart: D share by race level per election
            fig = px.bar(
                detail_df,
                x="election_date",
                y="d_share",
                color="race_level",
                barmode="group",
                color_discrete_map=LEVEL_COLORS,
                title="Dem Vote Share by Race Level",
                category_orders={"race_level": ["federal", "state", "county", "local"]},
            )
            fig.update_layout(yaxis_title="D Vote Share %", xaxis_title="Election")
            st.plotly_chart(fig, use_container_width=True)

            # Line chart: drop-off visualization
            fig2 = px.line(
                detail_df,
                x="race_level",
                y="d_share",
                color="election_date",
                markers=True,
                title="Dem Performance Drop-off: Federal \u2192 Local",
                category_orders={"race_level": ["federal", "state", "county", "local"]},
            )
            fig2.update_layout(yaxis_title="D Vote Share %", xaxis_title="Race Level")
            st.plotly_chart(fig2, use_container_width=True)

            # Drop-off summary
            if not summary_df.empty:
                st.subheader("Drop-off from Federal D Share (percentage points lost)")
                dropoff_cols = [c for c in summary_df.columns if "dropoff" in c]
                display_cols = ["election_date"] + [c for c in summary_df.columns if "d_share" in c] + dropoff_cols
                available_cols = [c for c in display_cols if c in summary_df.columns]
                st.dataframe(
                    summary_df[available_cols].style.format(precision=1, na_rep="No D candidate"),
                    hide_index=True,
                )

            st.warning("Missing bars or 'No D candidate' indicates no Democrat ran at that level, "
                       "not zero vote share.")

            with st.expander("View detailed data"):
                st.dataframe(detail_df, hide_index=True)
        else:
            st.info("No downballot data available for the selected election.")
    else:
        st.info("No general elections from 2016+ found in the database.")

# ============================================================
# TAB 8: Straight-Ticket Trends
# ============================================================
with tab8:
    st.header("Straight-Ticket Voting Trends")
    st.markdown("*Analyzing party loyalty and split-ticket behavior*")

    precinct_detail, trend_summary = get_straight_ticket_analysis()

    if not trend_summary.empty:
        # County-wide trend: D vs R straight-ticket votes
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=trend_summary["election_date"],
            y=trend_summary["total_d_straight"],
            name="D Straight Ticket",
            marker_color="blue",
        ))
        fig.add_trace(go.Bar(
            x=trend_summary["election_date"],
            y=trend_summary["total_r_straight"],
            name="R Straight Ticket",
            marker_color="red",
        ))
        fig.update_layout(
            barmode="group",
            title="Straight-Ticket Voting Over Time",
            yaxis_title="Total Straight-Ticket Votes",
            xaxis_title="Election",
        )
        st.plotly_chart(fig, use_container_width=True)

        # D share of straight-ticket trend
        valid_trend = trend_summary[trend_summary["d_straight_pct"].notna()]
        if not valid_trend.empty:
            fig2 = px.line(
                valid_trend,
                x="election_date",
                y="d_straight_pct",
                title="D Share of Straight-Ticket Votes Over Time",
                markers=True,
            )
            fig2.add_hline(y=50, line_dash="dash", line_color="gray", opacity=0.5)
            fig2.update_layout(yaxis_title="D % of Straight-Ticket Votes")
            st.plotly_chart(fig2, use_container_width=True)

        # Straight-ticket as % of total ballots
        ballot_trend = trend_summary[trend_summary["straight_pct_of_ballots"].notna()]
        if not ballot_trend.empty:
            fig3 = px.line(
                ballot_trend,
                x="election_date",
                y="straight_pct_of_ballots",
                title="Straight-Ticket Votes as % of Total Ballots",
                markers=True,
            )
            fig3.update_layout(yaxis_title="Straight-Ticket % of Ballots")
            st.plotly_chart(fig3, use_container_width=True)

        # Precinct-level heatmap of straight-ticket D%
        if not precinct_detail.empty:
            st.subheader("Precinct-Level Straight-Ticket D%")
            valid_pct = precinct_detail[precinct_detail["d_straight_pct"].notna()]
            if not valid_pct.empty:
                pivot = valid_pct.pivot_table(
                    index="precinct",
                    columns="election_date",
                    values="d_straight_pct",
                )
                # Sort by average D straight %
                pivot["_avg"] = pivot.mean(axis=1)
                pivot = pivot.sort_values("_avg", ascending=False)
                pivot = pivot.drop(columns=["_avg"])

                fig4 = px.imshow(
                    pivot,
                    color_continuous_scale=["red", "white", "blue"],
                    color_continuous_midpoint=50,
                    title="Straight-Ticket D% by Precinct Over Time",
                    labels=dict(x="Election", y="Precinct", color="D Straight %"),
                    aspect="auto",
                )
                fig4.update_layout(height=max(500, len(pivot) * 14))
                st.plotly_chart(fig4, use_container_width=True)

        with st.expander("View trend summary data"):
            st.dataframe(trend_summary, hide_index=True)

        with st.expander("View precinct detail data"):
            st.dataframe(precinct_detail, hide_index=True)
    else:
        st.info("No straight-ticket voting data available.")

# ============================================================
# TAB 9: Competitive Races (existing)
# ============================================================
with tab9:
    st.header("Competitive Races")
    margin_threshold = st.slider("Max margin (percentage points)", 5, 30, 15)

    competitive = get_competitive_races(min_margin=margin_threshold)
    if not competitive.empty:
        fig = px.scatter(
            competitive,
            x="election_date",
            y="margin",
            color="margin",
            size=competitive["total_votes"].abs(),
            hover_data=["race_name", "dem_votes", "rep_votes"],
            color_continuous_scale=["red", "white", "blue"],
            color_continuous_midpoint=0,
            title=f"Races Within {margin_threshold} Points"
        )
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(competitive)
    else:
        st.info("No competitive races found with current threshold.")

# Footer
st.markdown("---")
st.markdown("*BCD Election Data Tool | Designed for Boone County Democrats, reusable for any county*")
