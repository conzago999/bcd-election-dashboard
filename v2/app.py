"""
Indiana County Election Dashboard — V2
Multi-county Streamlit dashboard with county picker.
Run with: streamlit run v2/app.py --server.port 8502
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from database import get_connection, DB_PATH
from analysis import (
    get_all_counties,
    get_county_overview,
    get_dem_share_summary,
    get_competitive_races,
    get_uncontested_races,
    get_precinct_d_share,
    get_precinct_shift,
    get_target_races_2026,
    get_statewide_summary,
    get_statewide_d_share,
    get_statewide_uncontested_rate,
)

st.set_page_config(
    page_title="Indiana Election Dashboard",
    page_icon="\U0001f5f3\ufe0f",
    layout="wide",
)

# ─── Check Database ───────────────────────────────────────────────────────

if not os.path.exists(DB_PATH):
    st.error("Database not found. Run `python v2/etl.py` first.")
    st.stop()

# ─── County Picker ────────────────────────────────────────────────────────

all_counties = get_all_counties()
if all_counties.empty:
    st.error("No data in database. Run `python v2/etl.py` first.")
    st.stop()

st.title("Indiana County Election Dashboard")
st.markdown("*Precinct-level election analysis for every Indiana county*")

# Build county labels with metadata
county_labels = {}
for _, row in all_counties.iterrows():
    badge = ""
    if row["has_precinct_data"]:
        badge = f" ({row['precinct_count']} precincts, {row['election_count']} elections)"
    else:
        badge = f" (county-level, {row['election_count']} elections)"
    county_labels[row["name"]] = row["name"] + badge

county_names = list(county_labels.keys())
default_idx = county_names.index("Boone") if "Boone" in county_names else 0

selected_county = st.sidebar.selectbox(
    "Select County",
    county_names,
    index=default_idx,
    format_func=lambda x: county_labels[x],
    key="v2_county_picker",
)

# ─── Overview Sidebar ─────────────────────────────────────────────────────

overview = get_county_overview(selected_county)
if overview:
    st.sidebar.markdown("---")
    st.sidebar.metric("Precincts", overview["precinct_count"])
    st.sidebar.metric("Total Races", overview["total_races"])
    if not overview["has_precinct_data"]:
        st.sidebar.warning("County-level data only (no precinct breakdown)")

# ─── Tabs ─────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Overview", "Race Analysis", "Precinct Intel", "2026 Targets", "Statewide View",
])


# ═══════════════════════════════════════════════════════════════════════════
# TAB 1: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════

with tab1:
    if not overview:
        st.warning(f"No data found for {selected_county}")
    else:
        st.header(f"{selected_county} County — Overview")

        # KPI cards
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Elections", len(overview["elections"]))
        with col2:
            st.metric("Precincts", overview["precinct_count"])
        with col3:
            st.metric("Total Races", overview["total_races"])
        with col4:
            data_quality = "Precinct" if overview["has_precinct_data"] else "County"
            st.metric("Data Level", data_quality)

        # Election history table
        st.subheader("Election History")
        elections_df = overview["elections"]
        if not elections_df.empty:
            display_df = elections_df[["election_name", "race_count", "total_votes", "data_level"]].copy()
            display_df.columns = ["Election", "Races", "Total Votes", "Data Level"]
            display_df["Total Votes"] = display_df["Total Votes"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "")
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        # D vote share trend
        st.subheader("Democratic Vote Share Trend")
        summary = get_dem_share_summary(selected_county)
        if not summary.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=summary["election_date"],
                y=summary["overall_d_share"],
                mode="lines+markers",
                name="Overall D Share",
                line=dict(color="#1f77b4", width=3),
                marker=dict(size=10),
            ))
            fig.add_hline(y=50, line_dash="dash", line_color="gray", annotation_text="50%")
            fig.update_layout(
                yaxis_title="D Vote Share (%)",
                xaxis_title="",
                yaxis=dict(range=[0, max(60, summary["overall_d_share"].max() + 5)]),
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

            # Context
            latest = summary.iloc[-1]
            st.markdown(
                f"**Latest:** {latest['election_name']} — D share: **{latest['overall_d_share']:.1f}%** "
                f"across {int(latest['contested_races'])} contested races"
            )
        else:
            st.info("No contested D vs R races found for this county.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2: RACE ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

with tab2:
    st.header(f"{selected_county} County — Race Analysis")

    section = st.radio(
        "Section", ["Competitive Races", "Uncontested Races"],
        horizontal=True, key="v2_race_section",
    )

    if section == "Competitive Races":
        st.subheader("Races Where Democrats Were Competitive (35-55%)")
        competitive = get_competitive_races(selected_county)
        if competitive.empty:
            st.info("No competitive races found.")
        else:
            # Group by race level
            for level in ["federal", "state", "county", "municipal", "judicial", "school_board", "township", "other"]:
                level_df = competitive[competitive["race_level"] == level]
                if level_df.empty:
                    continue
                st.markdown(f"**{level.replace('_', ' ').title()} Races**")
                display = level_df[["election_name", "race_name", "d_votes", "r_votes", "d_share"]].copy()
                display.columns = ["Election", "Race", "D Votes", "R Votes", "D Share %"]
                st.dataframe(display, use_container_width=True, hide_index=True)

            # Chart: competitive races
            fig = px.scatter(
                competitive,
                x="election_date",
                y="d_share",
                color="race_level",
                hover_data=["race_name", "election_name"],
                title="Competitive Race D Share Over Time",
            )
            fig.add_hline(y=50, line_dash="dash", line_color="gray")
            fig.update_layout(yaxis_title="D Vote Share (%)", height=500)
            st.plotly_chart(fig, use_container_width=True)

    elif section == "Uncontested Races":
        st.subheader("Races With No Democratic Candidate")
        uncontested = get_uncontested_races(selected_county)
        if uncontested.empty:
            st.info("No uncontested R-only races found (or no general election data).")
        else:
            total_gen_races = len(get_competitive_races(selected_county, min_d_share=0, max_d_share=100))
            st.metric(
                "Uncontested Races",
                len(uncontested),
                help="Races with only Republican candidates (no D challenger)",
            )
            display = uncontested[["election_name", "race_name", "race_level"]].copy()
            display.columns = ["Election", "Race", "Level"]
            st.dataframe(display, use_container_width=True, hide_index=True)

            # By election
            by_election = uncontested.groupby("election_name").size().reset_index(name="uncontested_count")
            fig = px.bar(
                by_election,
                x="election_name",
                y="uncontested_count",
                title="Uncontested Races by Election",
                color_discrete_sequence=["#d62728"],
            )
            fig.update_layout(yaxis_title="Races Without D Candidate", xaxis_title="")
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3: PRECINCT INTEL
# ═══════════════════════════════════════════════════════════════════════════

with tab3:
    st.header(f"{selected_county} County — Precinct Intel")

    if not overview or not overview["has_precinct_data"]:
        st.warning(
            f"{selected_county} County only has county-level data in the state system. "
            "Precinct breakdowns require data from the county clerk."
        )
    else:
        # Election picker
        elections_list = overview["elections"]["election_name"].tolist()
        if not elections_list:
            st.info("No elections found.")
        else:
            precinct_section = st.radio(
                "Section", ["Precinct Rankings", "Precinct Shift"],
                horizontal=True, key="v2_precinct_section",
            )

            if precinct_section == "Precinct Rankings":
                sel_election = st.selectbox(
                    "Election", elections_list, key="v2_precinct_election"
                )

                precinct_df = get_precinct_d_share(selected_county, sel_election)
                if precinct_df.empty:
                    st.info("No precinct-level contested race data for this election.")
                else:
                    # Bar chart — split into D-winning and R-winning for clear colors
                    sorted_df = precinct_df.sort_values("avg_d_share", ascending=True).copy()
                    sorted_df["color"] = sorted_df["avg_d_share"].apply(
                        lambda x: "#2196F3" if x >= 50 else "#E53935"
                    )
                    fig = px.bar(
                        sorted_df,
                        x="avg_d_share",
                        y="precinct_name",
                        orientation="h",
                        color="color",
                        color_discrete_map="identity",
                        text=sorted_df["avg_d_share"].apply(lambda x: f"{x:.1f}%"),
                    )
                    fig.add_vline(x=50, line_dash="dash", line_color="white", line_width=2)
                    fig.update_layout(
                        title=f"Avg D Share by Precinct — {sel_election}",
                        xaxis_title="D Vote Share (%)",
                        yaxis_title="",
                        xaxis=dict(range=[0, max(65, sorted_df["avg_d_share"].max() + 5)]),
                        height=max(400, len(sorted_df) * 22),
                        showlegend=False,
                    )
                    fig.update_traces(textposition="outside")
                    st.plotly_chart(fig, use_container_width=True)

                    # Table
                    display = precinct_df.sort_values("avg_d_share", ascending=False).copy()
                    display.columns = ["Election", "Precinct", "Avg D Share %", "Races Counted", "Total Votes"]
                    display["Total Votes"] = display["Total Votes"].apply(lambda x: f"{int(x):,}")
                    st.dataframe(display, use_container_width=True, hide_index=True)

            elif precinct_section == "Precinct Shift":
                if len(elections_list) < 2:
                    st.info("Need at least 2 elections to compare shifts.")
                else:
                    col1, col2 = st.columns(2)
                    with col1:
                        older = st.selectbox("From (older)", elections_list[1:], key="v2_shift_old")
                    with col2:
                        newer = st.selectbox("To (newer)", elections_list[:1], key="v2_shift_new")

                    shift_df = get_precinct_shift(selected_county, older, newer)
                    if shift_df.empty:
                        st.info("No overlapping precinct data between these elections.")
                    else:
                        sorted_shift = shift_df.sort_values("shift", ascending=True)
                        colors = ["#1f77b4" if x > 0 else "#d62728" for x in sorted_shift["shift"]]
                        fig = go.Figure(go.Bar(
                            x=sorted_shift["shift"],
                            y=sorted_shift["precinct_name"],
                            orientation="h",
                            marker_color=colors,
                            text=sorted_shift["shift"].apply(lambda x: f"{x:+.1f}"),
                            textposition="outside",
                        ))
                        fig.add_vline(x=0, line_color="gray")
                        fig.update_layout(
                            title=f"D Share Shift: {older} to {newer}",
                            xaxis_title="D Share Change (points)",
                            height=max(400, len(sorted_shift) * 22),
                        )
                        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4: 2026 TARGETS
# ═══════════════════════════════════════════════════════════════════════════

with tab4:
    st.header(f"{selected_county} County — 2026 Target Races")

    targets = get_target_races_2026(selected_county)

    target_section = st.radio(
        "Section", ["Target Board", "Recruitment Opportunities"],
        horizontal=True, key="v2_target_section",
    )

    if target_section == "Target Board":
        target_df = targets["targets"]
        if target_df.empty:
            st.info("No historical D performance data to generate targets.")
        else:
            # High priority
            high = target_df[target_df["priority"] == "High"]
            medium = target_df[target_df["priority"] == "Medium"]

            if not high.empty:
                st.subheader("High Priority (Avg D share >= 45%)")
                display = high[["race_name", "race_level", "avg_d_share", "max_d_share", "elections_contested"]].copy()
                display.columns = ["Race", "Level", "Avg D%", "Best D%", "Times Contested"]
                display["Avg D%"] = display["Avg D%"].apply(lambda x: f"{x:.1f}%")
                display["Best D%"] = display["Best D%"].apply(lambda x: f"{x:.1f}%")
                st.dataframe(display, use_container_width=True, hide_index=True)

            if not medium.empty:
                st.subheader("Medium Priority (Avg D share 38-45%)")
                display = medium[["race_name", "race_level", "avg_d_share", "max_d_share", "elections_contested"]].copy()
                display.columns = ["Race", "Level", "Avg D%", "Best D%", "Times Contested"]
                display["Avg D%"] = display["Avg D%"].apply(lambda x: f"{x:.1f}%")
                display["Best D%"] = display["Best D%"].apply(lambda x: f"{x:.1f}%")
                st.dataframe(display, use_container_width=True, hide_index=True)

            if high.empty and medium.empty:
                st.info("No races with D share above 38% threshold found.")

            # Chart
            winnable = target_df[target_df["avg_d_share"] >= 30].copy()
            if not winnable.empty:
                fig = px.scatter(
                    winnable,
                    x="elections_contested",
                    y="avg_d_share",
                    color="priority",
                    hover_data=["race_name"],
                    color_discrete_map={"High": "#1f77b4", "Medium": "#ff7f0e", "Low": "#aec7e8"},
                    title="Target Races: D Share vs. Times Contested",
                )
                fig.add_hline(y=50, line_dash="dash", line_color="gray", annotation_text="Win line")
                fig.update_layout(height=500, xaxis_title="Elections Contested", yaxis_title="Avg D Share %")
                st.plotly_chart(fig, use_container_width=True)

    elif target_section == "Recruitment Opportunities":
        st.subheader("Races That Need a Democratic Candidate")
        uncontested = targets["uncontested"]
        if uncontested.empty:
            st.success("No uncontested R-only races found.")
        else:
            st.metric("Uncontested Races (Historical)", len(uncontested))
            by_level = uncontested.groupby("race_level").size().reset_index(name="count")
            fig = px.pie(
                by_level,
                values="count",
                names="race_level",
                title="Uncontested Races by Level",
            )
            st.plotly_chart(fig, use_container_width=True)

            display = uncontested[["election_name", "race_name", "race_level"]].copy()
            display.columns = ["Election", "Race", "Level"]
            st.dataframe(display, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 5: STATEWIDE VIEW
# ═══════════════════════════════════════════════════════════════════════════

with tab5:
    st.header("Statewide View — All 92 Counties")

    state_section = st.radio(
        "Section", ["County Rankings", "Data Coverage", "Uncontested Rates"],
        horizontal=True, key="v2_state_section",
    )

    if state_section == "County Rankings":
        st.subheader("Democratic Performance by County")
        d_share = get_statewide_d_share()
        if d_share.empty:
            st.info("No statewide D share data available.")
        else:
            # Horizontal bar chart
            sorted_df = d_share.sort_values("avg_d_share", ascending=True)
            colors = ["#1f77b4" if x >= 45 else ("#ff7f0e" if x >= 35 else "#d62728") for x in sorted_df["avg_d_share"]]
            fig = go.Figure(go.Bar(
                x=sorted_df["avg_d_share"],
                y=sorted_df["county"],
                orientation="h",
                marker_color=colors,
                text=sorted_df["avg_d_share"].apply(lambda x: f"{x:.1f}%"),
                textposition="outside",
            ))
            fig.add_vline(x=50, line_dash="dash", line_color="gray")
            fig.update_layout(
                title="Average D Vote Share by County (All General Elections)",
                xaxis_title="Avg D Vote Share %",
                height=max(600, len(sorted_df) * 22),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Top 10 most D-friendly
            st.subheader("Top 10 Most Competitive Counties for Democrats")
            top10 = d_share.head(10)
            display = top10[["county", "avg_d_share", "elections"]].copy()
            display.columns = ["County", "Avg D Share %", "Elections Analyzed"]
            display["Avg D Share %"] = display["Avg D Share %"].apply(lambda x: f"{x:.1f}%")
            st.dataframe(display, use_container_width=True, hide_index=True)

    elif state_section == "Data Coverage":
        st.subheader("Data Quality Across Indiana")
        summary = get_statewide_summary()
        if summary.empty:
            st.info("No statewide data.")
        else:
            col1, col2, col3 = st.columns(3)
            with col1:
                precinct_yes = summary["has_precinct_data"].sum()
                st.metric("Counties w/ Precinct Data", f"{precinct_yes}/92")
            with col2:
                st.metric("Total Precincts", f"{summary['precinct_count'].sum():,}")
            with col3:
                st.metric("Total Races", f"{summary['races'].sum():,}")

            # Coverage tiers
            def tier(row):
                if row["precinct_election_count"] >= 4:
                    return "Ready (4+ elections)"
                elif row["precinct_election_count"] >= 1:
                    return "Partial (1-3 elections)"
                else:
                    return "County-level only"

            summary["tier"] = summary.apply(tier, axis=1)
            tier_counts = summary.groupby("tier").size().reset_index(name="count")
            fig = px.pie(
                tier_counts,
                values="count",
                names="tier",
                title="Data Coverage Tiers",
                color="tier",
                color_discrete_map={
                    "Ready (4+ elections)": "#2ca02c",
                    "Partial (1-3 elections)": "#ff7f0e",
                    "County-level only": "#d62728",
                },
            )
            st.plotly_chart(fig, use_container_width=True)

            # Full table
            display = summary[["county", "has_precinct_data", "precinct_count", "precinct_election_count", "races", "tier"]].copy()
            display.columns = ["County", "Has Precincts", "Precinct Count", "Elections w/ Precincts", "Total Races", "Tier"]
            display["Has Precincts"] = display["Has Precincts"].map({1: "Yes", 0: "No"})
            st.dataframe(display, use_container_width=True, hide_index=True)

    elif state_section == "Uncontested Rates":
        st.subheader("Uncontested Race Rates by County")
        uncontested = get_statewide_uncontested_rate()
        if uncontested.empty:
            st.info("No uncontested data available.")
        else:
            sorted_df = uncontested.sort_values("uncontested_pct", ascending=True)
            fig = go.Figure(go.Bar(
                x=sorted_df["uncontested_pct"],
                y=sorted_df["county"],
                orientation="h",
                marker_color=sorted_df["uncontested_pct"].apply(
                    lambda x: "#d62728" if x > 60 else ("#ff7f0e" if x > 40 else "#2ca02c")
                ),
                text=sorted_df["uncontested_pct"].apply(lambda x: f"{x:.0f}%"),
                textposition="outside",
            ))
            fig.update_layout(
                title="% of General Election Races Without a D Candidate",
                xaxis_title="Uncontested Rate (%)",
                height=max(600, len(sorted_df) * 22),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Summary stats
            avg_rate = uncontested["uncontested_pct"].mean()
            worst = uncontested.iloc[0]
            st.markdown(
                f"**Statewide average:** {avg_rate:.1f}% of general election races have no D candidate. "
                f"Worst: **{worst['county']} County** ({worst['uncontested_pct']:.0f}%)"
            )


# ─── Footer ───────────────────────────────────────────────────────────────

st.sidebar.markdown("---")
st.sidebar.caption(
    "Data: IN Secretary of State ENR Archive (2016-2024). "
    "13 elections, 92 counties, 1M+ result rows."
)
st.sidebar.caption("V2 — Indiana Statewide Edition")
