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
    get_area_election_summary,
    get_2026_target_races,
)
from census_acs import get_area_demographics, get_tract_detail
from campaign_finance import (
    get_boone_county_contributions,
    get_contribution_summary,
    get_top_committees,
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
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "The Big Picture",
    "Precinct Intel",
    "Where to Win",
    "Voting Patterns",
    "Data Explorer",
    "Demographics",
    "Campaign Finance",
    "2026 Prep",
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

            # Use emoji indicators for confidence (dark-mode friendly)
            confidence_icons = {"high": "HIGH", "medium": "MEDIUM", "low": "LOW"}
            display_df["Confidence"] = display_df["Confidence"].apply(
                lambda x: confidence_icons.get(x.lower(), x) if isinstance(x, str) else x
            )
            display_df["Score"] = display_df["Score"].apply(lambda x: f"{x:.0%}")

            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                height=min(800, 35 * len(display_df) + 38),
                column_config={
                    "Score": st.column_config.TextColumn("Score"),
                    "Confidence": st.column_config.TextColumn("Confidence"),
                },
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

                    display_races = races_df[[
                        "race_name", "normalized_name", "race_level",
                        "race_type", "total_votes", "candidates", "precincts", "name_changed"
                    ]].copy()
                    # Add a visual indicator column for changed names
                    display_races["Changed"] = display_races["name_changed"].apply(
                        lambda x: "Yes" if x else ""
                    )
                    display_races = display_races.drop(columns=["name_changed"])
                    display_races.columns = [
                        "Original Name", "Normalized Name", "Level",
                        "Type", "Total Votes", "Candidates", "Precincts", "Changed"
                    ]

                    st.dataframe(
                        display_races,
                        use_container_width=True,
                        hide_index=True,
                    )

                    if changed_count > 0:
                        st.caption("Rows marked 'Yes' in Changed column have normalized names that differ from the original PDF data.")
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
            dq_df["Score"] = dq_df["Score"].apply(lambda x: f"{x:.0%}" if isinstance(x, (int, float)) else x)

            st.dataframe(
                dq_df,
                use_container_width=True,
                hide_index=True,
                height=min(800, 35 * len(dq_df) + 38),
                column_config={
                    "Score": st.column_config.TextColumn("Score"),
                    "Confidence": st.column_config.TextColumn("Confidence"),
                },
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


# ============================================================
# TAB 6: DEMOGRAPHICS
# ============================================================
with tab6:
    st.header("Demographics")
    st.markdown("*Census data correlated with voting patterns — what drives Democratic performance?*")

    # Load Census API key
    census_api_key = None
    try:
        census_api_key = st.secrets["census"]["api_key"]
    except Exception:
        # Try environment variable as fallback
        census_api_key = os.environ.get("CENSUS_API_KEY")

    if not census_api_key:
        st.warning("Census API key not configured. Add it to `.streamlit/secrets.toml` under `[census]`.")
        st.code('[census]\napi_key = "your_key_here"', language="toml")
        st.stop()

    demo_section = st.radio(
        "Section",
        ["Community Profile", "Demographics vs. Voting", "Tract Detail"],
        horizontal=True,
        key="demo_section"
    )

    # --- Community Profile ---
    if demo_section == "Community Profile":
        st.subheader("Boone County Community Profile")
        st.markdown("*Census ACS 5-Year estimates aggregated to geographic areas*")

        @st.cache_data(ttl=86400)
        def load_area_demographics(key):
            return get_area_demographics(key)

        area_demo = load_area_demographics(census_api_key)

        if not area_demo.empty:
            # KPI cards
            total_pop = area_demo["population"].sum()
            avg_income = int((area_demo["median_income"] * area_demo["population"] / total_pop).sum())
            avg_ed = round((area_demo["pct_bachelors"] * area_demo["population"] / total_pop).sum(), 1)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("County Population", f"{total_pop:,}")
            col2.metric("Avg Median Income", f"${avg_income:,}")
            col3.metric("Avg % Bachelor's+", f"{avg_ed}%")
            col4.metric("Geographic Areas", len(area_demo))

            st.divider()

            # Side-by-side bar charts
            col_left, col_right = st.columns(2)

            with col_left:
                fig_income = px.bar(
                    area_demo.sort_values("median_income"),
                    x="median_income",
                    y="area",
                    orientation="h",
                    color="median_income",
                    color_continuous_scale=["#f39c12", "#27ae60"],
                    title="Median Household Income by Area",
                    labels={"median_income": "Median Income", "area": ""},
                )
                fig_income.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig_income, use_container_width=True)

            with col_right:
                fig_ed = px.bar(
                    area_demo.sort_values("pct_bachelors"),
                    x="pct_bachelors",
                    y="area",
                    orientation="h",
                    color="pct_bachelors",
                    color_continuous_scale=["#e74c3c", "#3498db"],
                    title="% with Bachelor's Degree or Higher",
                    labels={"pct_bachelors": "% Bachelor's+", "area": ""},
                )
                fig_ed.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig_ed, use_container_width=True)

            col_left2, col_right2 = st.columns(2)

            with col_left2:
                fig_age = px.bar(
                    area_demo.sort_values("median_age"),
                    x="median_age",
                    y="area",
                    orientation="h",
                    color="median_age",
                    color_continuous_scale=["#3498db", "#8e44ad"],
                    title="Median Age by Area",
                    labels={"median_age": "Median Age", "area": ""},
                )
                fig_age.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig_age, use_container_width=True)

            with col_right2:
                fig_home = px.bar(
                    area_demo.sort_values("median_home_value"),
                    x="median_home_value",
                    y="area",
                    orientation="h",
                    color="median_home_value",
                    color_continuous_scale=["#95a5a6", "#2ecc71"],
                    title="Median Home Value by Area",
                    labels={"median_home_value": "Median Home Value", "area": ""},
                )
                fig_home.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig_home, use_container_width=True)

            # Full data table
            with st.expander("View full area demographics data"):
                display_demo = area_demo.copy()
                display_demo["median_income"] = display_demo["median_income"].apply(lambda x: f"${x:,}")
                display_demo["median_home_value"] = display_demo["median_home_value"].apply(lambda x: f"${x:,}")
                display_demo["population"] = display_demo["population"].apply(lambda x: f"{x:,}")
                st.dataframe(display_demo, use_container_width=True, hide_index=True)

            st.caption("Source: U.S. Census Bureau, ACS 5-Year Estimates (2022). "
                       "Areas are aggregations of census tracts mapped to Boone County geographic regions.")
        else:
            st.error("Failed to load Census data. Check your API key.")

    # --- Demographics vs. Voting ---
    elif demo_section == "Demographics vs. Voting":
        st.subheader("Demographics vs. Voting Patterns")
        st.markdown("*How do community characteristics correlate with Democratic performance?*")

        @st.cache_data(ttl=86400)
        def load_demo_voting(key):
            demos = get_area_demographics(key)
            votes = get_area_election_summary()
            if demos.empty or votes.empty:
                return pd.DataFrame()
            merged = demos.merge(votes, on="area", how="inner")
            return merged

        merged = load_demo_voting(census_api_key)

        if not merged.empty and len(merged) >= 3:
            # Variable selector
            demo_variables = {
                "Median Income": "median_income",
                "% Bachelor's Degree": "pct_bachelors",
                "Median Age": "median_age",
                "% Homeowner": "pct_owner_occupied",
                "Median Home Value": "median_home_value",
                "% White": "pct_white",
                "% Age 65+": "pct_65plus",
            }

            selected_var = st.selectbox(
                "Demographic variable to compare",
                list(demo_variables.keys()),
                key="demo_variable"
            )
            var_col = demo_variables[selected_var]

            # Main scatter: demographic vs D share
            fig = px.scatter(
                merged,
                x=var_col,
                y="overall_d_share",
                text="area",
                size="population",
                color="overall_d_share",
                color_continuous_scale=["#e74c3c", "#f39c12", "#3498db"],
                title=f"{selected_var} vs. Democratic Vote Share by Area",
                labels={var_col: selected_var, "overall_d_share": "D Vote Share %"},
                trendline="ols",
            )
            fig.update_traces(textposition="top center", textfont_size=10)
            fig.update_layout(height=500, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

            # Compute correlation
            corr = merged[var_col].corr(merged["overall_d_share"])
            direction = "positive" if corr > 0 else "negative"
            strength = "strong" if abs(corr) > 0.7 else "moderate" if abs(corr) > 0.4 else "weak"

            st.info(f"**Correlation: r = {corr:.2f}** ({strength} {direction}) — "
                    f"Areas with higher {selected_var.lower()} tend to have "
                    f"{'higher' if corr > 0 else 'lower'} Democratic vote share.")

            # Secondary scatter: demographic vs turnout (if available)
            if "avg_turnout" in merged.columns and merged["avg_turnout"].notna().any():
                fig2 = px.scatter(
                    merged,
                    x=var_col,
                    y="avg_turnout",
                    text="area",
                    size="population",
                    color="avg_turnout",
                    color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                    title=f"{selected_var} vs. Average Turnout by Area",
                    labels={var_col: selected_var, "avg_turnout": "Avg Turnout %"},
                    trendline="ols",
                )
                fig2.update_traces(textposition="top center", textfont_size=10)
                fig2.update_layout(height=500, showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)

                corr2 = merged[var_col].corr(merged["avg_turnout"])
                st.info(f"**Turnout correlation: r = {corr2:.2f}** — "
                        f"Areas with higher {selected_var.lower()} tend to have "
                        f"{'higher' if corr2 > 0 else 'lower'} turnout.")

            # Summary table
            with st.expander("View merged demographics + voting data"):
                display_cols = ["area", "population", var_col, "overall_d_share",
                                "precincts", "elections_counted"]
                if "avg_turnout" in merged.columns:
                    display_cols.append("avg_turnout")
                st.dataframe(merged[display_cols], use_container_width=True, hide_index=True)

            st.caption("Note: Correlations are based on 5 geographic areas. "
                       "With more granular precinct-to-tract mapping, these correlations will sharpen. "
                       "D Vote Share calculated from D vs R votes in general election federal/state/county races.")
        elif not merged.empty:
            st.warning("Not enough matched areas for correlation analysis. Need at least 3 areas with both demographic and election data.")
        else:
            st.error("Failed to load or merge demographic and election data.")

    # --- Tract Detail ---
    elif demo_section == "Tract Detail":
        st.subheader("Census Tract Detail")
        st.markdown("*All 11 Boone County census tracts with full demographic data*")

        @st.cache_data(ttl=86400)
        def load_tract_detail(key):
            return get_tract_detail(key)

        tracts = load_tract_detail(census_api_key)

        if not tracts.empty:
            # Summary metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("Census Tracts", len(tracts))
            col2.metric("Total Population", f"{tracts['total_population'].sum():,}")
            col3.metric("Areas Covered", tracts["area"].nunique())

            # Bar chart: population by tract
            fig = px.bar(
                tracts.sort_values("total_population"),
                x="total_population",
                y="area_detail",
                orientation="h",
                color="area",
                title="Population by Census Tract",
                labels={"total_population": "Population", "area_detail": "Tract"},
            )
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

            # Scatter: income vs education by tract
            fig2 = px.scatter(
                tracts,
                x="median_income",
                y="pct_bachelors",
                text="area_detail",
                size="total_population",
                color="area",
                title="Median Income vs. Education Level by Census Tract",
                labels={"median_income": "Median Household Income",
                        "pct_bachelors": "% Bachelor's Degree+"},
            )
            fig2.update_traces(textposition="top center", textfont_size=8)
            fig2.update_layout(height=500)
            st.plotly_chart(fig2, use_container_width=True)

            # Full data table
            display_tracts = tracts[[
                "area_detail", "area", "total_population", "median_income",
                "median_age", "pct_bachelors", "pct_white", "pct_65plus",
                "pct_owner_occupied", "median_home_value"
            ]].copy()
            display_tracts.columns = [
                "Tract", "Area", "Population", "Median Income",
                "Median Age", "% Bachelor's+", "% White", "% 65+",
                "% Owner-Occupied", "Median Home Value"
            ]
            st.dataframe(display_tracts, use_container_width=True, hide_index=True)

            st.caption("Source: U.S. Census Bureau, ACS 5-Year Estimates (2022). "
                       "Tract codes: 8101-8107, with 8106 split into 4 sub-tracts (Zionsville/Whitestown growth area).")
        else:
            st.error("Failed to load tract data. Check your API key.")


# ============================================================
# TAB 7: CAMPAIGN FINANCE
# ============================================================
with tab7:
    st.header("Campaign Finance")
    st.markdown("*Indiana state campaign contributions from Boone County donors (2018-2024)*")

    @st.cache_data(ttl=86400, show_spinner="Downloading campaign finance data...")
    def load_campaign_finance():
        return get_boone_county_contributions()

    finance_data = load_campaign_finance()

    if not finance_data.empty:
        finance_section = st.radio(
            "Section",
            ["Overview", "Party Breakdown", "Top Recipients", "Donor Geography"],
            horizontal=True,
            key="finance_section"
        )

        # --- Overview ---
        if finance_section == "Overview":
            st.subheader("Boone County Donor Overview")

            # KPI cards
            total_amount = finance_data["Amount"].sum()
            total_contributions = len(finance_data)
            unique_donors = finance_data["Name"].nunique()
            years_covered = sorted(finance_data["year"].unique())

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Contributions", f"${total_amount:,.0f}")
            col2.metric("Donation Count", f"{total_contributions:,}")
            col3.metric("Unique Donors", f"{unique_donors:,}")
            col4.metric("Years Covered", f"{len(years_covered)}")

            st.divider()

            # Trend over time
            yearly = finance_data.groupby("year").agg(
                total=("Amount", "sum"),
                count=("Amount", "count"),
                donors=("Name", "nunique"),
                avg=("Amount", "mean"),
            ).reset_index()

            col_left, col_right = st.columns(2)

            with col_left:
                fig = px.bar(
                    yearly,
                    x="year",
                    y="total",
                    color="total",
                    color_continuous_scale=["#3498db", "#2ecc71"],
                    title="Total Contributions by Year",
                    labels={"year": "Year", "total": "Total ($)"},
                    text=yearly["total"].apply(lambda x: f"${x:,.0f}"),
                )
                fig.update_traces(textposition="outside")
                fig.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

            with col_right:
                fig2 = px.bar(
                    yearly,
                    x="year",
                    y="donors",
                    color="donors",
                    color_continuous_scale=["#e74c3c", "#f39c12"],
                    title="Unique Donors by Year",
                    labels={"year": "Year", "donors": "Unique Donors"},
                    text="donors",
                )
                fig2.update_traces(textposition="outside")
                fig2.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)

            # Average contribution trend
            fig3 = px.line(
                yearly,
                x="year",
                y="avg",
                markers=True,
                title="Average Contribution Size Over Time",
                labels={"year": "Year", "avg": "Avg Contribution ($)"},
            )
            fig3.update_layout(height=350)
            st.plotly_chart(fig3, use_container_width=True)

            st.caption("Source: Indiana Election Division bulk contribution data. "
                       "Filtered to core Boone County ZIP codes (46035, 46050, 46052, 46069, 46071, 46075, 46077).")

        # --- Party Breakdown ---
        elif finance_section == "Party Breakdown":
            st.subheader("Estimated Party Breakdown")
            st.markdown("*Contributions classified as D/R based on committee name keywords. "
                        "'Unknown' includes PACs, local races, and committees without clear party affiliation.*")

            summary = get_contribution_summary(finance_data)

            if not summary.empty:
                # Stacked bar: D vs R vs Unknown by year
                party_colors = {"D": "#3498db", "R": "#e74c3c", "Unknown": "#95a5a6"}

                fig = px.bar(
                    summary,
                    x="year",
                    y="total_amount",
                    color="party_est",
                    barmode="stack",
                    color_discrete_map=party_colors,
                    title="Total Contributions by Party & Year",
                    labels={"year": "Year", "total_amount": "Total ($)", "party_est": "Party"},
                )
                fig.update_layout(height=450)
                st.plotly_chart(fig, use_container_width=True)

                # Donor count comparison
                fig2 = px.bar(
                    summary,
                    x="year",
                    y="unique_donors",
                    color="party_est",
                    barmode="group",
                    color_discrete_map=party_colors,
                    title="Unique Donors by Party & Year",
                    labels={"year": "Year", "unique_donors": "Unique Donors", "party_est": "Party"},
                )
                fig2.update_layout(height=400)
                st.plotly_chart(fig2, use_container_width=True)

                # D vs R comparison metrics
                st.divider()
                st.subheader("D vs R Summary (All Years Combined)")
                d_total = summary[summary["party_est"] == "D"]["total_amount"].sum()
                r_total = summary[summary["party_est"] == "R"]["total_amount"].sum()
                d_donors = summary[summary["party_est"] == "D"]["unique_donors"].sum()
                r_donors = summary[summary["party_est"] == "R"]["unique_donors"].sum()

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("D Total", f"${d_total:,.0f}")
                col2.metric("R Total", f"${r_total:,.0f}")
                col3.metric("D Donors", f"{d_donors:,}")
                col4.metric("R Donors", f"{r_donors:,}")

                if d_donors > 0 and r_donors > 0:
                    d_avg = d_total / d_donors
                    r_avg = r_total / r_donors
                    col1, col2, col3 = st.columns(3)
                    col1.metric("D Avg/Donor", f"${d_avg:,.0f}")
                    col2.metric("R Avg/Donor", f"${r_avg:,.0f}")
                    col3.metric("R:D Ratio", f"{r_total/d_total:.1f}x")

                    st.info(f"**Key insight:** Republican donors in Boone County give "
                            f"${r_avg:,.0f} per donor on average vs ${d_avg:,.0f} for Democrats — "
                            f"a {r_avg/d_avg:.1f}x difference. But Democrats have {d_donors:,} unique donors "
                            f"vs {r_donors:,} for Republicans — a broader base.")

                with st.expander("View party summary data"):
                    st.dataframe(summary, use_container_width=True, hide_index=True)

        # --- Top Recipients ---
        elif finance_section == "Top Recipients":
            st.subheader("Top Recipient Committees")
            st.markdown("*Where Boone County donors send their money*")

            top_n = st.slider("Number of committees to show", 10, 30, 15, key="finance_top_n")
            top = get_top_committees(finance_data, top_n=top_n)

            if not top.empty:
                party_colors = {"D": "#3498db", "R": "#e74c3c", "Unknown": "#95a5a6"}

                fig = px.bar(
                    top.sort_values("total_amount"),
                    x="total_amount",
                    y="Committee",
                    orientation="h",
                    color="party_est",
                    color_discrete_map=party_colors,
                    title=f"Top {top_n} Recipient Committees (All Years)",
                    labels={"total_amount": "Total ($)", "Committee": "", "party_est": "Party"},
                    hover_data=["unique_donors", "contribution_count", "years_active"],
                )
                fig.update_layout(height=max(500, top_n * 28))
                st.plotly_chart(fig, use_container_width=True)

                with st.expander("View committee data"):
                    display_top = top.copy()
                    display_top["total_amount"] = display_top["total_amount"].apply(lambda x: f"${x:,.0f}")
                    st.dataframe(display_top, use_container_width=True, hide_index=True)

        # --- Donor Geography ---
        elif finance_section == "Donor Geography":
            st.subheader("Donor Geography")
            st.markdown("*Where in Boone County are political donors?*")

            by_zip = finance_data.groupby(["zip_clean", "area", "year"]).agg(
                total=("Amount", "sum"),
                count=("Amount", "count"),
                donors=("Name", "nunique"),
            ).reset_index()

            # Aggregate by area across all years
            by_area_all = finance_data.groupby("area").agg(
                total=("Amount", "sum"),
                count=("Amount", "count"),
                donors=("Name", "nunique"),
                d_amount=("Amount", lambda x: x[finance_data.loc[x.index, "party_est"] == "D"].sum()),
                r_amount=("Amount", lambda x: x[finance_data.loc[x.index, "party_est"] == "R"].sum()),
            ).reset_index()
            by_area_all["d_pct"] = (by_area_all["d_amount"] / by_area_all["total"] * 100).round(1)
            by_area_all["avg_donation"] = (by_area_all["total"] / by_area_all["count"]).round(0)

            col_left, col_right = st.columns(2)

            with col_left:
                fig = px.bar(
                    by_area_all.sort_values("total"),
                    x="total",
                    y="area",
                    orientation="h",
                    color="total",
                    color_continuous_scale=["#f39c12", "#2ecc71"],
                    title="Total Contributions by Area",
                    labels={"total": "Total ($)", "area": ""},
                )
                fig.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

            with col_right:
                fig2 = px.bar(
                    by_area_all.sort_values("donors"),
                    x="donors",
                    y="area",
                    orientation="h",
                    color="donors",
                    color_continuous_scale=["#e74c3c", "#3498db"],
                    title="Unique Donors by Area",
                    labels={"donors": "Unique Donors", "area": ""},
                )
                fig2.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)

            # D share of donations by area
            fig3 = px.bar(
                by_area_all.sort_values("d_pct"),
                x="d_pct",
                y="area",
                orientation="h",
                color="d_pct",
                color_continuous_scale=["#e74c3c", "#3498db"],
                title="Democratic Share of Identifiable Donations by Area",
                labels={"d_pct": "D Share %", "area": ""},
            )
            fig3.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig3, use_container_width=True)

            with st.expander("View geographic data"):
                st.dataframe(by_area_all, use_container_width=True, hide_index=True)

            st.caption("Areas match the Census demographic areas. "
                       "D/R classification is estimated from committee names and may undercount both parties.")
    else:
        st.error("Failed to load campaign finance data. Check your internet connection.")


# ============================================================
# TAB 8: 2026 ELECTION PREP
# ============================================================
with tab8:
    st.header("2026 Election Prep")
    st.markdown("""
    **Indiana 2026 Midterm: Primary May 5 · General Nov 3**

    This section identifies which Boone County seats are up in 2026 and uses historical data
    to prioritize where Democratic candidates can compete. Races are classified by historical
    D performance from the 2014, 2018, and 2022 midterm cycles.
    """)

    prep_section = st.radio(
        "Section",
        ["Target Board", "Recruitment Targets", "Trending Races", "Historical Detail"],
        horizontal=True,
        key="prep_section",
    )

    target_data = get_2026_target_races()

    if target_data["races"].empty:
        st.warning("Could not load 2026 target data.")
    else:
        races = target_data["races"]
        top_opps = target_data["top_opportunities"]
        uncontested = target_data["uncontested_history"]
        trend_races = target_data["trend_races"]
        history = target_data["history"]

        if prep_section == "Target Board":
            st.subheader("2026 Target Board")
            st.markdown("All expected 2026 races, prioritized by D competitiveness.")

            # KPI row
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            with kpi1:
                st.metric("Total Races", len(races))
            with kpi2:
                high_count = len(races[races["priority"] == "High"])
                st.metric("High Priority", high_count)
            with kpi3:
                recruit_count = len(races[races["priority"] == "Recruit"])
                st.metric("Need Candidate", recruit_count)
            with kpi4:
                trending = len(trend_races)
                st.metric("Trending D", trending)

            # Priority filter
            priority_filter = st.multiselect(
                "Filter by priority",
                ["High", "Medium", "Recruit", "Low"],
                default=["High", "Medium", "Recruit"],
                key="prep_priority_filter",
            )

            level_filter = st.multiselect(
                "Filter by level",
                sorted(races["level"].unique()),
                default=sorted(races["level"].unique()),
                key="prep_level_filter",
            )

            filtered = races[
                (races["priority"].isin(priority_filter)) &
                (races["level"].isin(level_filter))
            ].copy()

            # Display columns
            display_cols = ["race", "level", "priority", "latest_d_pct", "d_trend", "d_contested", "action"]
            display_df = filtered[display_cols].copy()
            display_df = display_df.rename(columns={
                "race": "Race",
                "level": "Level",
                "priority": "Priority",
                "latest_d_pct": "Last D%",
                "d_trend": "D Trend",
                "d_contested": "D Contested",
                "action": "Recommended Action",
            })

            st.dataframe(
                display_df.sort_values(["Priority", "Last D%"], ascending=[True, False]),
                use_container_width=True,
                hide_index=True,
                height=600,
            )

            # Top opportunities chart
            chart_data = top_opps[top_opps["latest_d_pct"].notna()].copy()
            if not chart_data.empty:
                st.subheader("Top D Opportunities (by Last D Vote Share)")
                fig = px.bar(
                    chart_data.sort_values("latest_d_pct"),
                    x="latest_d_pct",
                    y="race",
                    orientation="h",
                    color="latest_d_pct",
                    color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                    range_color=[25, 55],
                    title="",
                    labels={"latest_d_pct": "D Two-Party %", "race": ""},
                )
                fig.add_vline(x=50, line_dash="dash", line_color="gray",
                              annotation_text="50% (Win)", annotation_position="top")
                fig.update_layout(height=max(400, len(chart_data) * 35), showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        elif prep_section == "Recruitment Targets":
            st.subheader("Candidate Recruitment Targets")
            st.markdown("""
            These races had **no Democratic candidate in 2022**. Fielding a candidate — even in
            a tough race — builds name recognition, develops the bench, and forces R to spend resources.
            Races with historical D performance are higher-priority recruitment targets.
            """)

            if uncontested.empty:
                st.info("All 2022 races were contested by D candidates!")
            else:
                # Split into tiers
                has_history = uncontested[uncontested["latest_d_pct"].notna()]
                no_history = uncontested[uncontested["latest_d_pct"].isna()]

                if not has_history.empty:
                    st.markdown("#### Previously Contested — Need Candidate Again")
                    st.markdown("These races had a D candidate in a prior cycle but went uncontested in 2022.")
                    display_cols = ["race", "level", "latest_d_pct", "latest_d_year", "d_contested", "action"]
                    display_df = has_history[display_cols].rename(columns={
                        "race": "Race", "level": "Level", "latest_d_pct": "Last D%",
                        "latest_d_year": "Last D Year", "d_contested": "D Contested",
                        "action": "Recommended Action",
                    })
                    st.dataframe(display_df, use_container_width=True, hide_index=True)

                if not no_history.empty:
                    st.markdown("#### Never Contested — New Territory")
                    st.markdown("These races have never had a D challenger in our data (2014-2022). "
                                "Township boards and trustees are often good entry-level races for new candidates.")
                    display_cols = ["race", "level", "action"]
                    display_df = no_history[display_cols].rename(columns={
                        "race": "Race", "level": "Level", "action": "Recommended Action",
                    })
                    st.dataframe(display_df, use_container_width=True, hide_index=True)

                # Summary stats
                st.markdown("---")
                col1, col2 = st.columns(2)
                with col1:
                    by_level = uncontested.groupby("level").size().reset_index(name="count")
                    fig = px.pie(
                        by_level,
                        names="level",
                        values="count",
                        title="Uncontested R Races by Level",
                        color_discrete_sequence=px.colors.qualitative.Set2,
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    st.markdown("#### Recruitment Priority")
                    st.markdown(f"- **{len(has_history)}** races with prior D performance — re-recruit")
                    st.markdown(f"- **{len(no_history)}** races never contested — build from scratch")
                    st.markdown(f"- **{len(uncontested)}** total recruitment targets")
                    st.markdown("")
                    st.markdown("**Tip:** Township trustee and board races are low-cost, high-value "
                                "recruitment targets. They require minimal fundraising and build the "
                                "local candidate pipeline.")

        elif prep_section == "Trending Races":
            st.subheader("Races Trending Democratic")
            st.markdown("These races show D vote share **increasing** across midterm cycles (2014→2018→2022).")

            if trend_races.empty:
                st.info("No races with measurable D trend across multiple cycles.")
            else:
                display_cols = ["race", "level", "latest_d_pct", "d_trend", "d_contested", "priority"]
                display_df = trend_races[display_cols].rename(columns={
                    "race": "Race", "level": "Level", "latest_d_pct": "Last D%",
                    "d_trend": "D Trend (pts)", "d_contested": "D Contested", "priority": "Priority",
                })
                st.dataframe(display_df, use_container_width=True, hide_index=True)

                # Trend chart: show D% across cycles for trending races
                trending_names = trend_races["race"].head(10).tolist()
                trend_history = history[
                    (history["normalized_name"].isin(trending_names)) &
                    (history["d_2party_pct"].notna()) &
                    (history["d_2party_pct"] > 0)
                ].copy()

                if not trend_history.empty:
                    fig = px.line(
                        trend_history,
                        x="year",
                        y="d_2party_pct",
                        color="normalized_name",
                        markers=True,
                        title="D Vote Share Trend Across Midterm Cycles",
                        labels={"d_2party_pct": "D Two-Party %", "year": "Election Year",
                                "normalized_name": "Race"},
                    )
                    fig.add_hline(y=50, line_dash="dash", line_color="gray")
                    fig.update_layout(height=500)
                    st.plotly_chart(fig, use_container_width=True)

                    st.caption("Only races with D candidates in 2+ midterm cycles are shown. "
                               "Trend = change from first to last contested midterm cycle.")

        elif prep_section == "Historical Detail":
            st.subheader("Full Historical Race Data")
            st.markdown("Browse D performance for every race across midterm generals (2014, 2018, 2022).")

            # Pivot: race × year → D%
            contested_history = history[
                (history["d_2party_pct"].notna()) &
                (history["d_2party_pct"] > 0)
            ].copy()

            if contested_history.empty:
                st.info("No contested D vs R race data available.")
            else:
                pivot = contested_history.pivot_table(
                    index=["normalized_name", "race_level"],
                    columns="year",
                    values="d_2party_pct",
                ).reset_index()
                pivot.columns.name = None
                pivot = pivot.rename(columns={
                    "normalized_name": "Race",
                    "race_level": "Level",
                })

                # Add trend column if 2022 and at least one prior year exist
                year_cols = [c for c in pivot.columns if c in ["2014", "2018", "2022"]]
                if len(year_cols) >= 2:
                    def calc_trend(row):
                        vals = [row[y] for y in year_cols if pd.notna(row[y])]
                        if len(vals) >= 2:
                            return round(vals[-1] - vals[0], 1)
                        return None
                    pivot["Trend"] = pivot.apply(calc_trend, axis=1)

                pivot = pivot.sort_values("Level")
                st.dataframe(pivot, use_container_width=True, hide_index=True, height=600)

                # All races by level
                st.markdown("---")
                st.subheader("All 2022 Races by Level")
                st.markdown("Complete list of races from 2022 (which will cycle again in 2026).")

                level_counts = races.groupby("level").size().reset_index(name="count")
                fig = px.bar(
                    level_counts.sort_values("count", ascending=False),
                    x="level",
                    y="count",
                    color="level",
                    title="2026 Expected Races by Level",
                    labels={"level": "Race Level", "count": "Number of Races"},
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

                with st.expander("View all 2022 races (2026 preview)"):
                    all_display = races[["race", "level", "parties_2022", "candidates_2022"]].rename(columns={
                        "race": "Race", "level": "Level", "parties_2022": "Parties (2022)",
                        "candidates_2022": "Candidates (2022)",
                    })
                    st.dataframe(all_display, use_container_width=True, hide_index=True, height=500)


# Footer
st.markdown("---")
st.markdown("*BCD Election Data Tool | Designed for Boone County Democrats, reusable for any county*")
