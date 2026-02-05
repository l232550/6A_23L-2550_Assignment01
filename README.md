# **NYC Congestion Pricing Analysis Pipeline** 🚕

**End-to-end analytics dashboard | 2.5M+ taxi trips | Q1 2025**

## **🎯 Key Findings**

| Metric | Value | Insight |
|--------|-------|---------|
| **Compliance Rate** | **87.2%** | Beats 85% target ✓ |
| **Rain Elasticity** | **+0.071** | Rain = 7.1% MORE trips |
| **Q1 Revenue** | **$18.7M** | On track for $75M annual |
| **Leakage Hotspots** | **Top 10 = 68%** | Clear evasion patterns |
| **Vendor Fraud** | **5 flagged** | Vendor 1: 2,847 ghost trips |

## **🚀 Quick Start** (1-click reproducible)

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline (fresh weather API)
python pipeline.py

# Launch live dashboard (9 interactive charts)
streamlit run dashboard/app.py
```

**Live at:** `http://localhost:8501`

## **📊 5-Phase Pipeline**

```
Phase 1: TLC Scraping → Ghost Filters → Dec Imputation
Phase 2: Leakage Detection → Q1 Volumes  
Phase 3: Border Effects → Velocity Heatmaps → Tip Economics
Phase 4: Open-Meteo API → Rain Elasticity → Vendor Audit
Phase 5: Streamlit Dashboard (9 charts + metrics)
```

**Tech Stack:**
```
2.5M+ TLC records (2023-2025) | DuckDB (10GB joins <2s)
Polars | Dask | Open-Meteo API | Streamlit | Plotly
```

## **📈 Live Dashboard Tabs**

| Tab | Analysis |
|-----|----------|
| **Congestion Impact** | Compliance + Q1 volume shift |
| **Spatial Patterns** | Border evasion + velocity heatmaps |
| **Economic Fairness** | Surcharge vs tip "double squeeze" |
| **Weather Effects** | Rain elasticity + vendor audit |

## **🔍 Audit Highlights**

```
Top Suspicious Vendors:
1. Vendor 1: 2,847 ghost trips (14.2%)
2. Vendor 2: 1,923 ghost trips (9.6%) 
3. Vendor 3: 1,456 ghost trips (7.3%)

Policy Rec: IMMEDIATE audit of Vendor 1
Expected ROI: $320K annual recovery
```

## **📄 Executive Summary**

> **Rain increases demand 7.1%** (+0.071 elasticity), validating surge pricing. **87% compliance** beats target. **$18.7M Q1 revenue** on pace. **Vendor 1 audit priority** (2,847 ghost trips = $80K leakage).

## **🛠 File Structure**

```
├── .gitignore              # Blocks 2.7GB data
├── requirements.txt        # pip install -r
├── config.py              # Paths + settings
├── pipeline.py            # 1-command full run
├── dashboard/app.py       # Live Streamlit dashboard
├── cleaning/ghost_filters.py
├── ingestion/tlc_scraper.py
├── processing/*.py        # Core analytics
└── reports/               # Audit outputs
```

## **✨ Policy Impact**

**Data validates NYC congestion pricing:**
- **Compliance working** (87% > 85% target)
- **Rain surge pricing opportunity** (+7.1% demand)
- **Vendor fraud detected** (Vendor 1 = immediate audit)
- **Driver economics tracked** (surcharge vs tip trends)

***

**Tooba Nadeem**  
*Software Engineering Junior | Data Science | Feb 2026*  
*Faisalabad, Pakistan*  
[🔗 Medium Blog](https://medium.com/@toobaanadeem/nyc-congestion-pricing-4-phase-data-deep-dive-8a8013e48424) | [💼 LinkedIn](https://www.linkedin.com/posts/tooba-nadeem_datascience-python-streamlit-activity-7425272743606165505-iuPS?utm_source=share&utm_medium=member_desktop&rcm=ACoAAFCDJ5UB_zltld3b-CgTQf-x9SOCP_6KTYA)

***

```bash
# Clone + Run (Anyone can reproduce!)
git clone https://github.com/l232550/6A_23L-2550_Assignment01.git
cd 6A_23L-2550_Assignment01
pip install -r requirements.txt
streamlit run dashboard/app.py
```

**⭐ Star if helpful!** 🚀
