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
    get_precinct_volatility,
    get_precinct_pvi,
    get_surge_voter_analysis,
    get_uncontested_race_mapping,
    get_third_party_persuadability,
    get_rolloff_analysis,
    get_straight_ticket_geography,
    get_headline_kpis,
    get_top_opportunities,
    get_election_overview,
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
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")
conn = get_connection()
elections = pd.read_sql_query("SELECT DISTINCT election_date, election_name FROM elections ORDER BY election_date", conn)
conn.close()

# ============================================================
# COLOR MAPS
# ============================================================
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

SURGE_COLORS = {
    "Growing + Bluing": "#2ecc71",
    "Growing + Reddening": "#e74c3c",
    "Stable + Bluing": "#3498db",
    "Stable + Reddening": "#95a5a6",
}

CONTEST_COLORS = {
    "Contested": "#3498db",
    "Uncontested D": "#2ecc71",
    "Uncontested R": "#e74c3c",
}

DEPENDENCY_COLORS = {
    "Brand-Dependent": "#8e44ad",
    "Mixed": "#f39c12",
    "Candidate-Dependent": "#2ecc71",
}

# ============================================================
# TABS (4 tabs)
# ============================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "The Big Picture",
    "Precinct Intel",
    "Where to Win",
    "Voting Patterns",
    "Data Explorer",
])

# ============================================================
# TAB 1: THE BIG PICTURE
# ============================================================
with tab1:
    st.header("The Big Picture")

    # --- Headline KPIs with deltas ---
    report = generate_summary_report()
    kpis = get_headline_kpis()

    if kpis:
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("Elections", report["total_elections"])
        col2.metric("Precincts", report["total_precincts"])
        col3.metric("D Vote Share",
                     f"{kpis.get('d_share_latest', 0)}%",
                     delta=f"{kpis.get('d_share_delta', 0):+.1f} pp")
        col4.metric("Avg Turnout",
                     f"{kpis.get('turnout_latest', 0)}%",
                     delta=f"{kpis.get('turnout_delta', 0):+.1f} pp")
        col5.metric("D Straight-Ticket %",
                     f"{kpis.get('straight_d_pct_latest', 0)}%",
                     delta=f"{kpis.get('straight_d_delta', 0):+.1f} pp")
        col6.metric("D Contested Races",
                     kpis.get("d_contested_latest", 0),
                     delta=f"{kpis.get('d_contested_delta', 0):+d}")
        st.caption(f"Deltas compare {kpis.get('latest_election', '?')} vs {kpis.get('prior_election', '?')}")
    else:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Elections", report["total_elections"])
        col2.metric("Races", report["total_races"])
        col3.metric("Candidates", report["total_candidates"])
        col4.metric("Precincts", report["total_precincts"])
    st.markdown(f"**Data spans:** {report['earliest_election']} to {report['latest_election']}")

    # --- Top 3 Strategic Opportunities ---
    st.divider()
    st.subheader("Top 3 Strategic Opportunities")
    opps = get_top_opportunities()
    if opps:
        opp_cols = st.columns(len(opps))
        for i, opp in enumerate(opps):
            with opp_cols[i]:
                st.markdown(f"**{i+1}. {opp['title']}**")
                st.markdown(opp["detail"])
                st.caption(f"Source: {opp['source']}")
    else:
        st.info("Not enough data to generate opportunity recommendations.")

    # --- Blue Shift Trends ---
    st.divider()
    st.subheader("Blue Shift Trends")
    st.markdown("*Tracking the blue shift in Boone County*")

    vote_shares = get_dem_vote_share_by_election()

    if not vote_shares.empty:
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

        with st.expander("View raw vote share data"):
            st.dataframe(vote_shares)
    else:
        st.info("No vote share data available yet.")


# ============================================================
# TAB 2: PRECINCT INTEL
# ============================================================
with tab2:
    st.header("Precinct Intel")
    st.markdown("*Deep dive into every precinct's political DNA*")

    intel_section = st.radio(
        "Section",
        ["Typology", "Heatmap", "Shift Comparison", "Volatility Index", "PVI", "Growth Analysis"],
        horizontal=True,
        key="intel_section"
    )

    # --- Typology ---
    if intel_section == "Typology":
        st.subheader("Precinct Targeting Typology")
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

            top_t = typology.attrs.get("top_threshold", "N/A")
            mid_t = typology.attrs.get("mid_threshold", "N/A")
            st.caption(f"Thresholds (from data): Best D \u2265 {top_t}% D share | "
                       f"Competitive/Trending D \u2265 {mid_t}% | Below = Lean R / Strong R")

            with st.expander("View typology data"):
                st.dataframe(typology, hide_index=True)
        else:
            st.info("No precinct data available for typology analysis.")

    # --- Heatmap ---
    elif intel_section == "Heatmap":
        st.subheader("Precinct Heatmap Over Time")
        st.markdown("*D margin by precinct across every election (blue = D advantage, red = R advantage)*")

        col1, col2 = st.columns(2)
        with col1:
            heatmap_min_elections = st.slider("Min elections for a precinct to appear", 1, 15, 3, key="hm_min")
        with col2:
            heatmap_sort = st.radio("Sort precincts by", ["Avg D margin", "Latest D margin", "Alphabetical"], key="hm_sort")

        heatmap = get_precinct_heatmap_data(min_elections=heatmap_min_elections)

        if not heatmap.empty:
            if heatmap_sort == "Latest D margin":
                last_col = heatmap.columns[-1]
                heatmap = heatmap.sort_values(last_col, ascending=False)
            elif heatmap_sort == "Alphabetical":
                heatmap = heatmap.sort_index(ascending=True)

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

    # --- Shift Comparison ---
    elif intel_section == "Shift Comparison":
        st.subheader("Precinct-Level Shift Analysis")
        st.markdown("*Compare Democratic performance between two elections*")

        if len(elections) >= 2:
            col1, col2 = st.columns(2)
            with col1:
                date1 = st.selectbox("Earlier election", elections["election_date"].tolist(), index=0, key="shift_date1")
            with col2:
                date2 = st.selectbox("Later election", elections["election_date"].tolist(),
                                    index=min(1, len(elections)-1), key="shift_date2")

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

    # --- Volatility Index (NEW) ---
    elif intel_section == "Volatility Index":
        st.subheader("Precinct Volatility Index")
        st.markdown("*Measures average election-to-election D share swing per precinct. "
                     "High volatility = persuadable voters who change behavior.*")

        vol_min_elections = st.slider("Min elections", 3, 15, 4, key="vol_min")
        volatility_df = get_precinct_volatility(min_elections=vol_min_elections)

        if not volatility_df.empty:
            fig = px.bar(
                volatility_df.sort_values("volatility", ascending=True),
                x="volatility",
                y="precinct",
                orientation="h",
                color="volatility",
                color_continuous_scale=["#2ecc71", "#f39c12", "#e74c3c"],
                title="Precinct Volatility Index (pp swing per election)",
                hover_data=["max_swing", "avg_d_share", "latest_d_share"],
            )
            fig.update_layout(height=max(500, len(volatility_df) * 16))
            st.plotly_chart(fig, use_container_width=True)

            # Context: scatter of volatility vs D share
            fig2 = px.scatter(
                volatility_df,
                x="avg_d_share",
                y="volatility",
                text="precinct",
                color="volatility",
                color_continuous_scale=["#2ecc71", "#f39c12", "#e74c3c"],
                title="Volatility vs. Avg D Share (top-right = high value persuasion targets)",
                hover_data=["latest_d_share", "max_swing"],
            )
            fig2.update_traces(textposition="top center", textfont_size=7)
            fig2.update_layout(
                xaxis_title="Avg D Vote Share %",
                yaxis_title="Volatility (pp swing/election)",
                height=500,
            )
            st.plotly_chart(fig2, use_container_width=True)

            with st.expander("View volatility data"):
                st.dataframe(volatility_df, hide_index=True)
        else:
            st.info("No data available for volatility analysis.")

    # --- PVI (NEW) ---
    elif intel_section == "PVI":
        st.subheader("Precinct Partisan Voting Index")
        st.markdown("*Local Cook PVI: each precinct compared to the county average "
                     "using presidential races only. Positive = more D than county, negative = more R.*")

        pvi_df = get_precinct_pvi()

        if not pvi_df.empty:
            fig = px.bar(
                pvi_df.sort_values("pvi"),
                x="pvi",
                y="precinct",
                orientation="h",
                color="pvi",
                color_continuous_scale=["#922b21", "#e74c3c", "white", "#3498db", "#1a5276"],
                color_continuous_midpoint=0,
                title="Precinct PVI (positive = more D than county avg)",
                hover_data=["pvi_label", "avg_d_share", "elections_counted"],
            )
            fig.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.5,
                          annotation_text="County Average")
            fig.update_layout(height=max(500, len(pvi_df) * 16))
            st.plotly_chart(fig, use_container_width=True)

            # Summary stats
            col1, col2, col3 = st.columns(3)
            d_lean = len(pvi_df[pvi_df["pvi"] > 0])
            r_lean = len(pvi_df[pvi_df["pvi"] < 0])
            col1.metric("D-Leaning Precincts", d_lean)
            col2.metric("R-Leaning Precincts", r_lean)
            col3.metric("Presidential Elections Used", pvi_df["elections_counted"].max())

            with st.expander("View PVI data"):
                st.dataframe(pvi_df, hide_index=True)
        else:
            st.info("No presidential race data available for PVI calculation.")

    # --- Growth Analysis (NEW) ---
    elif intel_section == "Growth Analysis":
        st.subheader("Surge Voter / Growth Analysis")
        st.markdown("*Tracks registration growth vs. D share change over time. "
                     "'Growing + Bluing' precincts are long-term strategic investments.*")

        surge_df = get_surge_voter_analysis()

        if not surge_df.empty:
            med_growth = surge_df.attrs.get("median_growth", surge_df["reg_growth_pct"].median())
            med_d = surge_df.attrs.get("median_d_change", surge_df["d_share_change"].median())

            fig = px.scatter(
                surge_df,
                x="reg_growth_pct",
                y="d_share_change",
                color="quadrant",
                color_discrete_map=SURGE_COLORS,
                hover_data=["precinct", "earliest_registered", "latest_registered"],
                title="Registration Growth vs. D Share Change by Precinct",
                text="precinct",
            )
            fig.add_hline(y=med_d, line_dash="dash", line_color="gray", opacity=0.5,
                          annotation_text=f"Median D change: {med_d:.1f} pp")
            fig.add_vline(x=med_growth, line_dash="dash", line_color="gray", opacity=0.5,
                          annotation_text=f"Median growth: {med_growth:.0f}%")
            fig.update_traces(textposition="top center", textfont_size=7)
            fig.update_layout(
                xaxis_title="Registration Growth %",
                yaxis_title="D Share Change (pp)",
                height=600,
            )
            st.plotly_chart(fig, use_container_width=True)

            # Quadrant summary
            quad_counts = surge_df["quadrant"].value_counts()
            cols = st.columns(4)
            for i, (quad, count) in enumerate(quad_counts.items()):
                cols[i % 4].metric(quad, count)

            # Growing + Bluing detail table
            growing_blue = surge_df[surge_df["quadrant"] == "Growing + Bluing"].sort_values(
                "reg_growth_pct", ascending=False
            )
            if not growing_blue.empty:
                st.subheader(f"Growing + Bluing Precincts ({len(growing_blue)})")
                st.caption("These precincts are both gaining residents AND trending more Democratic.")
                st.dataframe(
                    growing_blue[["precinct", "reg_growth_pct", "d_share_change",
                                  "earliest_registered", "latest_registered"]],
                    hide_index=True,
                )

            with st.expander("View all growth data"):
                st.dataframe(surge_df, hide_index=True)
        else:
            st.info("No turnout/registration data available for growth analysis.")


# ============================================================
# TAB 3: WHERE TO WIN
# ============================================================
with tab3:
    st.header("Where to Win")
    st.markdown("*Identify concrete opportunities for Democratic gains*")

    win_section = st.radio(
        "Section",
        ["Turnout Opportunities", "Competitive Races", "Uncontested Mapping",
         "Third-Party Persuadability", "Rolloff Analysis"],
        horizontal=True,
        key="win_section"
    )

    # --- Turnout Opportunities (existing) ---
    if win_section == "Turnout Opportunities":
        st.subheader("Turnout Opportunity Analysis")
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

            with st.expander("View all precinct data"):
                st.dataframe(turnout_dem, hide_index=True)

            st.caption("Turnout records exceeding the cap have been excluded as data quality outliers.")
        else:
            st.info("No turnout + vote share data available.")

    # --- Competitive Races (existing) ---
    elif win_section == "Competitive Races":
        st.subheader("Competitive Races")
        margin_threshold = st.slider("Max margin (percentage points)", 5, 30, 15, key="comp_margin")

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

    # --- Uncontested Mapping (NEW) ---
    elif win_section == "Uncontested Mapping":
        st.subheader("Uncontested Race Mapping")
        st.markdown("*Races where only one major party fielded a candidate. "
                     "Every uncontested R seat is a missed D opportunity.*")

        summary_df, detail_df = get_uncontested_race_mapping()

        if not summary_df.empty:
            # Stacked bar: contested vs uncontested by year
            status_cols = [c for c in summary_df.columns if c != "election_date"]
            melt = summary_df.melt(
                id_vars="election_date",
                value_vars=status_cols,
                var_name="status",
                value_name="count"
            )
            fig = px.bar(
                melt,
                x="election_date",
                y="count",
                color="status",
                barmode="stack",
                color_discrete_map=CONTEST_COLORS,
                title="Contested vs Uncontested Races by Election (General Elections Only)",
            )
            fig.update_layout(yaxis_title="Number of Races", xaxis_title="Election")
            st.plotly_chart(fig, use_container_width=True)

            # Summary metrics
            if "Uncontested R" in summary_df.columns:
                total_unc_r = int(summary_df["Uncontested R"].sum())
                latest_unc_r = int(summary_df.iloc[-1].get("Uncontested R", 0)) if len(summary_df) > 0 else 0
                col1, col2 = st.columns(2)
                col1.metric("Total Uncontested R Seats (All Years)", total_unc_r)
                col2.metric("Uncontested R in Latest Election", latest_unc_r)

        if not detail_df.empty and "estimated_latent_d_votes" in detail_df.columns:
            st.subheader("Uncontested R Seats - Candidate Recruitment Targets")
            st.caption("Latent D support estimated from D performance in contested "
                       "races at the same level in the same election.")
            display_cols = ["race_name", "race_level", "election_date",
                           "total_votes", "baseline_d_share", "estimated_latent_d_votes"]
            available = [c for c in display_cols if c in detail_df.columns]
            st.dataframe(
                detail_df[available].head(20),
                hide_index=True,
            )
        elif summary_df.empty:
            st.info("No uncontested race data available.")

    # --- Third-Party Persuadability (NEW) ---
    elif win_section == "Third-Party Persuadability":
        st.subheader("Third-Party Persuadability")
        st.markdown("*Precincts where L/I vote share is significant. "
                     "'Flippable' = elections where third-party votes exceeded the D-R margin.*")

        agg_df, detail_df = get_third_party_persuadability()

        if not agg_df.empty:
            fig = px.bar(
                agg_df.sort_values("avg_third_party_pct", ascending=True),
                x="avg_third_party_pct",
                y="precinct",
                orientation="h",
                color="avg_third_party_pct",
                color_continuous_scale=["#aed6f1", "#8e44ad"],
                title="Average Third-Party Vote Share by Precinct (General Elections)",
                hover_data=["avg_margin", "flippable_elections", "total_elections"],
            )
            fig.update_layout(height=max(500, len(agg_df) * 14))
            st.plotly_chart(fig, use_container_width=True)

            # Flippable precincts highlight
            flippable = agg_df[agg_df["flippable_elections"] > 0].sort_values(
                "flippable_elections", ascending=False
            )
            if not flippable.empty:
                st.subheader(f"Flippable Precincts ({len(flippable)})")
                st.caption("Precincts where third-party vote exceeded D-R margin in at least one election. "
                           "If these voters had chosen D, the result flips.")
                st.dataframe(flippable, hide_index=True)
            else:
                st.info("No flippable precincts found (third-party vote never exceeded D-R margin).")

            with st.expander("View per-election detail"):
                st.dataframe(detail_df, hide_index=True)
        else:
            st.info("No third-party voting data available.")

    # --- Rolloff Analysis (NEW) ---
    elif win_section == "Rolloff Analysis":
        st.subheader("Ballot Rolloff Analysis")
        st.markdown("*Rolloff = voters who cast a ballot but skip a downballot race. "
                     "High rolloff in D-leaning precincts = cheapest marginal votes (they're already at the polls).*")

        avg_df, heatmap_df = get_rolloff_analysis()

        if not heatmap_df.empty:
            fig = px.imshow(
                heatmap_df,
                color_continuous_scale=["white", "#f39c12", "#e74c3c"],
                zmin=0,
                zmax=50,
                title="Average Rolloff % by Precinct and Election",
                labels=dict(x="Election Date", y="Precinct", color="Rolloff %"),
                aspect="auto",
            )
            fig.update_layout(
                height=max(600, len(heatmap_df) * 16),
                xaxis=dict(tickangle=45),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Top rolloff precincts
            if not avg_df.empty:
                overall_avg = avg_df.groupby("precinct")["avg_rolloff"].mean().reset_index()
                overall_avg.columns = ["precinct", "overall_avg_rolloff"]
                overall_avg = overall_avg.sort_values("overall_avg_rolloff", ascending=False)

                st.subheader("Highest Average Rolloff Precincts")
                st.caption("These precincts consistently have voters who skip downballot races. "
                           "Voter education can capture these 'free' votes.")
                st.dataframe(overall_avg.head(15), hide_index=True)

            with st.expander("View full rolloff data"):
                st.dataframe(avg_df, hide_index=True)
        else:
            st.info("No rolloff data available.")


# ============================================================
# TAB 4: VOTING PATTERNS
# ============================================================
with tab4:
    st.header("Voting Patterns")
    st.markdown("*Understand party loyalty, ticket-splitting, and downballot behavior*")

    pattern_section = st.radio(
        "Section",
        ["Downballot Drop-off", "Straight-Ticket Trends", "Straight-Ticket Geography"],
        horizontal=True,
        key="pattern_section"
    )

    # --- Downballot Drop-off (existing) ---
    if pattern_section == "Downballot Drop-off":
        st.subheader("Downballot Drop-off Analysis")
        st.markdown("*Where does Dem support erode going from federal to local races?*")

        st.info("Race-level labels (federal/state/county/local) are only available for 2016+ general elections. "
                "Earlier elections have all races categorized as 'other'.")

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

    # --- Straight-Ticket Trends (existing) ---
    elif pattern_section == "Straight-Ticket Trends":
        st.subheader("Straight-Ticket Voting Trends")
        st.markdown("*Analyzing party loyalty and split-ticket behavior*")

        precinct_detail, trend_summary = get_straight_ticket_analysis()

        if not trend_summary.empty:
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

            if not precinct_detail.empty:
                st.subheader("Precinct-Level Straight-Ticket D%")
                valid_pct = precinct_detail[precinct_detail["d_straight_pct"].notna()]
                if not valid_pct.empty:
                    pivot = valid_pct.pivot_table(
                        index="precinct",
                        columns="election_date",
                        values="d_straight_pct",
                    )
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

    # --- Straight-Ticket Geography (NEW) ---
    elif pattern_section == "Straight-Ticket Geography":
        st.subheader("Straight-Ticket Geography")
        st.markdown("*Straight-ticket D votes as % of total D votes per precinct. "
                     "High % = party brand carries candidates. Low % = voters choose individual Ds.*")

        geo_df = get_straight_ticket_geography()

        if not geo_df.empty:
            geo_elections = sorted(geo_df["election_date"].unique())
            geo_election = st.selectbox(
                "Election", geo_elections,
                index=len(geo_elections)-1,
                key="geo_election"
            )
            filtered = geo_df[geo_df["election_date"] == geo_election].sort_values(
                "straight_d_pct_of_total"
            )

            if not filtered.empty:
                fig = px.bar(
                    filtered,
                    x="straight_d_pct_of_total",
                    y="precinct",
                    orientation="h",
                    color="dependency",
                    color_discrete_map=DEPENDENCY_COLORS,
                    title=f"Straight-Ticket D as % of Total D Votes ({geo_election})",
                    hover_data=["straight_d_votes", "total_d_votes"],
                )
                fig.add_vline(x=40, line_dash="dash", line_color="#8e44ad", opacity=0.3,
                              annotation_text="Brand-Dependent threshold")
                fig.add_vline(x=20, line_dash="dash", line_color="#2ecc71", opacity=0.3,
                              annotation_text="Candidate-Dependent threshold")
                fig.update_layout(height=max(500, len(filtered) * 16))
                st.plotly_chart(fig, use_container_width=True)

                # Dependency summary
                dep_counts = filtered["dependency"].value_counts()
                cols = st.columns(3)
                for i, (dep, count) in enumerate(dep_counts.items()):
                    cols[i % 3].metric(dep, count)

                st.caption("Brand-Dependent (\u226540%): Party label does the work. "
                           "Candidate-Dependent (<20%): Individual candidate appeal matters most. "
                           "Mixed (20-40%): Both factors contribute.")

            with st.expander("View all geography data"):
                st.dataframe(geo_df, hide_index=True)
        else:
            st.info("No straight-ticket geography data available.")


# ============================================================
# TAB 5: DATA EXPLORER
# ============================================================
with tab5:
    st.header("Data Explorer")
    st.markdown("*Browse and verify all raw election data*")

    explorer_section = st.radio(
        "Section",
        ["Elections Overview", "Race & Result Browser", "Data Quality Report"],
        horizontal=True,
        key="explorer_section"
    )

    # --- Elections Overview ---
    if explorer_section == "Elections Overview":
        st.subheader("All Elections")

        overview = get_election_overview()

        if not overview.empty:
            # Format for display
            display_df = overview[[
                "election_date", "election_type", "election_name",
                "race_count", "result_count", "precinct_count",
                "turnout_precincts", "confidence_level", "confidence_score"
            ]].copy()
            display_df.columns = [
                "Date", "Type", "Name", "Races", "Results",
                "Precincts", "Turnout Precincts", "Confidence", "Score"
            ]

            # Color-code confidence
            def highlight_confidence(row):
                level = row["Confidence"].lower() if isinstance(row["Confidence"], str) else ""
                if level == "high":
                    return ["background-color: #d4edda"] * len(row)
                elif level == "medium":
                    return ["background-color: #fff3cd"] * len(row)
                elif level == "low":
                    return ["background-color: #f8d7da"] * len(row)
                return [""] * len(row)

            st.dataframe(
                display_df.style.apply(highlight_confidence, axis=1).format(
                    {"Score": "{:.0%}"}
                ),
                use_container_width=True,
                hide_index=True,
                height=min(800, 35 * len(display_df) + 38),
            )

            # Drill-down: select an election to see its races
            st.divider()
            st.subheader("Election Detail")
            election_options = overview.apply(
                lambda r: f"{r['election_date']} — {r['election_name']}", axis=1
            ).tolist()
            selected_election = st.selectbox(
                "Select election to inspect",
                election_options,
                key="explorer_election"
            )

            if selected_election:
                sel_idx = election_options.index(selected_election)
                sel_election_id = int(overview.iloc[sel_idx]["election_id"])

                conn = get_connection()
                races_df = pd.read_sql_query("""
                    SELECT
                        r.race_name,
                        COALESCE(r.normalized_name, r.race_name) as normalized_name,
                        r.race_level,
                        r.race_type,
                        r.total_votes,
                        COUNT(DISTINCT c.id) as candidates,
                        COUNT(DISTINCT res.precinct_id) as precincts
                    FROM races r
                    LEFT JOIN results res ON res.race_id = r.id
                    LEFT JOIN candidates c ON res.candidate_id = c.id
                    WHERE r.election_id = ?
                    GROUP BY r.id
                    ORDER BY r.race_level, r.race_name
                """, conn, params=[sel_election_id])
                conn.close()

                if not races_df.empty:
                    # Flag rows where name was normalized
                    races_df["name_changed"] = races_df["race_name"] != races_df["normalized_name"]
                    changed_count = races_df["name_changed"].sum()

                    col1, col2, col3 = st.columns(3)
                    col1.metric("Races", len(races_df))
                    col2.metric("With Normalized Names", int(changed_count))
                    col3.metric("Race Levels", races_df["race_level"].nunique())

                    # Highlight changed names
                    def highlight_name_change(row):
                        if row["name_changed"]:
                            return ["background-color: #fff3cd"] * len(row)
                        return [""] * len(row)

                    display_races = races_df[[
                        "race_name", "normalized_name", "race_level",
                        "race_type", "total_votes", "candidates", "precincts"
                    ]].copy()
                    display_races.columns = [
                        "Original Name", "Normalized Name", "Level",
                        "Type", "Total Votes", "Candidates", "Precincts"
                    ]

                    st.dataframe(
                        display_races.style.apply(
                            lambda row: ["background-color: #fff3cd"] * len(row)
                            if races_df.iloc[row.name]["name_changed"] else [""] * len(row),
                            axis=1
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )

                    if changed_count > 0:
                        st.caption("Highlighted rows have normalized names that differ from the original PDF data.")
                else:
                    st.info("No races found for this election.")
        else:
            st.info("No elections in database.")

    # --- Race & Result Browser ---
    elif explorer_section == "Race & Result Browser":
        st.subheader("Race & Result Browser")
        st.markdown("*Drill into any race to see precinct-level results*")

        conn = get_connection()
        all_elections = pd.read_sql_query(
            "SELECT id, election_date, election_name FROM elections ORDER BY election_date",
            conn
        )
        conn.close()

        if not all_elections.empty:
            election_labels = all_elections.apply(
                lambda r: f"{r['election_date']} — {r['election_name']}", axis=1
            ).tolist()
            selected_el = st.selectbox(
                "Election", election_labels, key="explorer_election2"
            )
            sel_idx = election_labels.index(selected_el)
            sel_eid = int(all_elections.iloc[sel_idx]["id"])

            # Get races for this election
            conn = get_connection()
            race_list = pd.read_sql_query("""
                SELECT id, race_name, COALESCE(normalized_name, race_name) as display_name,
                       race_level, race_type
                FROM races WHERE election_id = ?
                ORDER BY race_level, race_name
            """, conn, params=[sel_eid])
            conn.close()

            if not race_list.empty:
                # Build race labels showing normalized name if different
                race_labels = []
                for _, r in race_list.iterrows():
                    label = r["display_name"]
                    if r["race_level"]:
                        label = f"[{r['race_level']}] {label}"
                    if r["display_name"] != r["race_name"]:
                        label += f" (was: {r['race_name']})"
                    race_labels.append(label)

                selected_race_label = st.selectbox(
                    "Race", race_labels, key="explorer_race"
                )
                sel_race_idx = race_labels.index(selected_race_label)
                sel_race_id = int(race_list.iloc[sel_race_idx]["id"])

                # Get results for this race
                conn = get_connection()
                results_df = pd.read_sql_query("""
                    SELECT
                        c.name as candidate,
                        c.party,
                        COALESCE(p.precinct_name, 'TOTAL') as precinct,
                        res.votes,
                        res.vote_percentage
                    FROM results res
                    JOIN candidates c ON res.candidate_id = c.id
                    LEFT JOIN precincts p ON res.precinct_id = p.id
                    WHERE res.race_id = ?
                    ORDER BY c.party, c.name, p.precinct_name
                """, conn, params=[sel_race_id])
                conn.close()

                if not results_df.empty:
                    # Summary: total votes per candidate
                    summary = results_df.groupby(["candidate", "party"])["votes"].sum().reset_index()
                    summary = summary.sort_values("votes", ascending=False)
                    total_votes = summary["votes"].sum()
                    summary["share"] = (summary["votes"] / total_votes * 100).round(1)

                    st.markdown("**Candidate Totals:**")
                    sum_cols = st.columns(min(len(summary), 6))
                    for i, (_, row) in enumerate(summary.iterrows()):
                        party_str = f" ({row['party']})" if row['party'] else ""
                        sum_cols[i % len(sum_cols)].metric(
                            f"{row['candidate']}{party_str}",
                            f"{int(row['votes']):,} votes",
                            f"{row['share']}%"
                        )

                    # Full precinct-level results
                    st.markdown("**Precinct-Level Results:**")
                    st.dataframe(
                        results_df,
                        use_container_width=True,
                        hide_index=True,
                        height=min(600, 35 * len(results_df) + 38),
                    )
                else:
                    st.info("No results found for this race.")

                # All races summary for this election
                with st.expander("View all races in this election"):
                    conn = get_connection()
                    all_races_summary = pd.read_sql_query("""
                        SELECT
                            COALESCE(r.normalized_name, r.race_name) as race,
                            r.race_level as level,
                            GROUP_CONCAT(DISTINCT c.party) as parties,
                            COUNT(DISTINCT c.id) as candidates,
                            SUM(res.votes) as total_votes,
                            COUNT(DISTINCT res.precinct_id) as precincts
                        FROM races r
                        LEFT JOIN results res ON res.race_id = r.id
                        LEFT JOIN candidates c ON res.candidate_id = c.id
                        WHERE r.election_id = ?
                        GROUP BY r.id
                        ORDER BY r.race_level, r.race_name
                    """, conn, params=[sel_eid])
                    conn.close()
                    st.dataframe(all_races_summary, use_container_width=True, hide_index=True)
            else:
                st.info("No races found for this election.")
        else:
            st.info("No elections in database.")

    # --- Data Quality Report ---
    elif explorer_section == "Data Quality Report":
        st.subheader("Data Quality Report")

        conn = get_connection()

        # KPI cards
        stats = pd.read_sql_query("""
            SELECT
                (SELECT COUNT(*) FROM elections) as total_elections,
                (SELECT COUNT(*) FROM races) as total_races,
                (SELECT COUNT(*) FROM results) as total_results,
                (SELECT COUNT(DISTINCT precinct_id) FROM results WHERE precinct_id IS NOT NULL) as total_precincts,
                (SELECT COUNT(*) FROM data_quality WHERE overall_confidence = 'high') as high_count,
                (SELECT COUNT(*) FROM data_quality) as assessed_count
        """, conn).iloc[0]

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Elections", int(stats["total_elections"]))
        high_pct = int(stats["high_count"] / stats["assessed_count"] * 100) if stats["assessed_count"] > 0 else 0
        col2.metric("HIGH Confidence", f"{high_pct}%",
                     f"{int(stats['high_count'])}/{int(stats['assessed_count'])} assessed")
        col3.metric("Total Races", f"{int(stats['total_races']):,}")
        col4.metric("Total Results", f"{int(stats['total_results']):,}")
        col5.metric("Precincts", int(stats["total_precincts"]))

        # Full data quality table
        st.divider()
        st.subheader("Confidence Scores by Election")
        dq_df = pd.read_sql_query("""
            SELECT
                e.election_date as "Date",
                e.election_name as "Election",
                dq.overall_confidence as "Confidence",
                dq.confidence_score as "Score",
                dq.source_type as "Source",
                CASE WHEN dq.cross_validated THEN 'Yes' ELSE 'No' END as "Cross-Validated",
                CASE WHEN dq.race_names_clean THEN 'Yes' ELSE 'No' END as "Names Clean",
                CASE WHEN dq.turnout_consistent THEN 'Yes' ELSE 'No' END as "Turnout OK",
                CASE WHEN dq.precinct_count_match THEN 'Yes' ELSE 'No' END as "Precinct Match",
                dq.notes as "Notes"
            FROM data_quality dq
            JOIN elections e ON e.id = dq.election_id
            ORDER BY e.election_date
        """, conn)

        if not dq_df.empty:
            def highlight_quality(row):
                level = row["Confidence"].lower() if isinstance(row["Confidence"], str) else ""
                if level == "high":
                    return ["background-color: #d4edda"] * len(row)
                elif level == "medium":
                    return ["background-color: #fff3cd"] * len(row)
                elif level == "low":
                    return ["background-color: #f8d7da"] * len(row)
                return [""] * len(row)

            st.dataframe(
                dq_df.style.apply(highlight_quality, axis=1).format(
                    {"Score": "{:.0%}"}
                ),
                use_container_width=True,
                hide_index=True,
                height=min(800, 35 * len(dq_df) + 38),
            )
        else:
            st.info("No data quality assessments found.")

        # Import log
        st.divider()
        st.subheader("Import Log")
        import_log = pd.read_sql_query("""
            SELECT
                filename as "File",
                file_type as "Type",
                records_imported as "Records",
                status as "Status",
                notes as "Notes",
                imported_at as "Imported"
            FROM import_log
            ORDER BY imported_at DESC
        """, conn)
        conn.close()

        if not import_log.empty:
            st.dataframe(import_log, use_container_width=True, hide_index=True)
        else:
            st.info("No import records found.")


# Footer
st.markdown("---")
st.markdown("*BCD Election Data Tool | Designed for Boone County Democrats, reusable for any county*")
