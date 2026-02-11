# BCD Election Data Dashboard

## What Is This?

An interactive election data dashboard for Boone County Democrats, built to help us make data-driven decisions about where to campaign, who to recruit, and how to win. It covers **23 elections from 2010-2024** with **41,752 precinct-level results** across **964 races**.

---

## Links

- **Live Dashboard:** https://bcd-election-dashboard-kdkkrbpyzwbqoqxk6jfjcf.streamlit.app
- **GitHub Repo:** https://github.com/conzago999/bcd-election-dashboard
- **Data Source:** All data comes from official Boone County Clerk election PDFs

---

## How It Works

The dashboard has **5 tabs**, each serving a different purpose:

### Tab 1: The Big Picture
The executive summary. Shows headline KPIs (D vote share, turnout, straight-ticket %, contested races) with trend deltas, the top 3 strategic opportunities auto-generated from the data, and the blue shift trend charts over time.

### Tab 2: Precinct Intel
Deep dive into every precinct's political DNA. Six sub-sections:
- **Typology** — Classifies each precinct (Best D, Trending D, Competitive, Lean R, Strong R)
- **Heatmap** — D margin by precinct across every election (visual matrix)
- **Shift Comparison** — Compare D performance between any two elections
- **Volatility Index** — Which precincts swing the most? (persuadable voters)
- **PVI** — Local Partisan Voting Index using presidential races
- **Growth Analysis** — Registration growth vs D share change (where are new voters going?)

### Tab 3: Where to Win
Concrete opportunities for Democratic gains. Five sub-sections:
- **Turnout Opportunities** — Low turnout + high D share = mobilization goldmines
- **Competitive Races** — Races within striking distance
- **Uncontested Mapping** — R seats we didn't even contest (recruitment targets)
- **Third-Party Persuadability** — Where L/I votes exceeded the D-R margin
- **Rolloff Analysis** — Voters who show up but skip downballot races

### Tab 4: Voting Patterns
Party loyalty and ticket-splitting behavior:
- **Downballot Drop-off** — Where D support erodes from federal to local
- **Straight-Ticket Trends** — D vs R straight-ticket voting over time
- **Straight-Ticket Geography** — Which precincts rely on party brand vs candidate appeal

### Tab 5: Data Explorer
Raw data browser for verification and research:
- **Elections Overview** — All 23 elections with confidence scores; drill into any election to see its races and which race names were cleaned up
- **Race & Result Browser** — Pick any election + race to see precinct-level vote counts for every candidate
- **Data Quality Report** — Confidence scoring breakdown, import log

---

## Data Coverage

### Elections in the Database (23 total)

| Year | Primary | General | Other |
|------|---------|---------|-------|
| 2010 | May 4 | Nov 2 | |
| 2011 | May 3 (Municipal) | Nov 8 (Municipal) | |
| 2012 | May 8 | Nov 6 | |
| 2013 | — | — | *No election data* |
| 2014 | May 6 | Nov 4 | |
| 2015 | May 5 (Municipal) | Nov 3 (Municipal) | |
| 2016 | May 3 | Nov 8 | |
| 2017 | | | May 2 (Special — Sheridan school referendum) |
| 2018 | May 8 | Nov 6 | |
| 2019 | May 7 | Nov 5 | |
| 2020 | — | Nov 3 | *No primary data* |
| 2021 | — | — | *No election data* |
| 2022 | — | Nov 8 | *No primary data* |
| 2023 | May 2 (Municipal) | Nov 7 | |
| 2024 | May 7 | Nov 5 | |

### By the Numbers
- **41,752** precinct-level vote results
- **964** individual races
- **217** unique precincts tracked
- **974** turnout records
- **100%** of results have precinct assignments (no orphaned data)
- **All 23 elections rated HIGH confidence** (avg score: 95.9%)

### Race Types Covered
- Local (municipal, township, town council): 285 races
- County (commissioner, council, sheriff, etc.): 256 races
- Party (precinct committeemen, delegates): 209 races
- State (state rep, senate, governor, etc.): 129 races
- Federal (president, US Senate, US House): 51 races
- Ballot measures (public questions, referendums): 34 races

---

## Data Quality Summary

All data comes from official Boone County Clerk digital PDFs. Each election is scored on 5 factors: source quality, race name cleanliness, turnout consistency, cross-validation against source, and precinct count accuracy.

- **23/23 elections** rated HIGH confidence
- **12 elections** individually cross-validated against source PDFs (candidate-by-candidate comparison)
- **Zero confirmed data entry errors** found during cross-validation
- **264 race names** were cleaned/normalized (originals preserved for reference)

### Known Gaps
- **2013 and 2021:** No election data (may not have had county-level races, or PDFs need to be sourced)
- **2020 and 2022 Primaries:** Not in the database yet
- **2016 Primary turnout:** Only county-wide summary, not precinct-level
- **School corp races (2010-2014):** Some span multiple counties; our data only has the Boone County portion

---

## Items for Abbey to Review

### Data Verification
- [ ] **Spot-check 2-3 elections in the Data Explorer tab** — Pick elections you're familiar with, use the Race & Result Browser to verify candidate names and vote counts look right
- [ ] **Check the 2011 elections** — These were re-imported from PDFs that were originally corrupted. Verify the races and candidates look correct for the 2011 Lebanon/Zionsville municipal races
- [ ] **Check the 2023 Municipal Primary** — This was upgraded from summary-only to full precinct data. Verify the 42 precincts and 22 races look complete
- [ ] **Review the normalized race names** — In Elections Overview, drill into any 2010-2014 election. Rows marked "Yes" in the Changed column had their names cleaned up. Verify the normalized versions are correct (e.g., "County Comm Dist 1" → "County Commissioner District 1")

### Missing Data to Source
- [ ] **2013 election data** — Were there Boone County races in 2013? If so, do we have the PDFs?
- [ ] **2020 Primary** — Do we have the PDF from the Boone County Clerk?
- [ ] **2021 election data** — Any municipal or special elections?
- [ ] **2022 Primary** — Do we have this PDF?
- [ ] **Voter registration data** — The Growth Analysis section needs registration snapshots over time. Do we have this from the Clerk or the voter file?

### Strategic Review
- [ ] **Review the Top 3 Opportunities** on the Big Picture tab — Do these match your on-the-ground knowledge?
- [ ] **Check the Precinct Typology** — Are the "Best D" and "Trending D" precincts the ones we'd expect? Any surprises?
- [ ] **Look at Uncontested Mapping** — Which uncontested R seats should we prioritize for candidate recruitment?
- [ ] **Review Turnout Opportunities** — Do the "Mobilization Goldmine" precincts align with where we have volunteer capacity?

### Technical / Access
- [ ] **Verify the live dashboard link works** for you: https://bcd-election-dashboard-kdkkrbpyzwbqoqxk6jfjcf.streamlit.app
- [ ] **Flag any charts or tables that are confusing** — We can add explanatory text or simplify
- [ ] **Identify any additional analyses** you'd want to see in the dashboard

---

## How to Use the Dashboard

1. Go to the live link above
2. The 5 tabs across the top are your main navigation
3. Within Tabs 2-4, use the radio buttons to switch between sub-sections
4. Most charts are interactive — hover for details, click legend items to show/hide
5. Look for "View data" expanders below charts to see the raw numbers
6. Use the Data Explorer tab (Tab 5) to browse any election, race, or candidate directly

---

## Tech Details (for reference)

- **Built with:** Python, Streamlit, Plotly, SQLite, Pandas
- **Database:** SQLite file (bcd_elections.db) — portable, no server needed
- **Hosting:** Streamlit Community Cloud (free tier, auto-deploys from GitHub)
- **Source code:** https://github.com/conzago999/bcd-election-dashboard
