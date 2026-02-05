import sys
import os

# Add project root to path (must come BEFORE importing config)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# import config safely
from config import PROCESSED_DIR, DATA_DIR

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from streamlit_folium import st_folium


st.set_page_config(
    page_title="NYC Congestion Analysis", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CUSTOM CSS FOR STRIKING DESIGN
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Load the external CSS
local_css(os.path.join(os.path.dirname(__file__), "style.css"))


# HERO SECTION
st.markdown("""
    <div class="hero-section">
        <h1>🚕 NYC Congestion Pricing Analysis</h1>
        <p>
            Deep-dive investigation into Manhattan's toll zone impact across revenue, compliance, weather, and driver economics
        </p>
    </div>
""", unsafe_allow_html=True)

# Load all CSVs
@st.cache_data
def load_data():
    return {
        'leakage': pd.read_csv(f"{PROCESSED_DIR}/leakage_audit.csv"),
        'top_leakage': pd.read_csv(f"{PROCESSED_DIR}/top_leakage_pickups.csv"),
        'q1_volumes': pd.read_csv(f"{PROCESSED_DIR}/q1_2024_vs_2025.csv"),
        'border': pd.read_csv(f"{PROCESSED_DIR}/border_effect.csv"),
        'velocity_24': pd.read_csv(f"{PROCESSED_DIR}/velocity_heatmap_q1_2024.csv"),
        'velocity_25': pd.read_csv(f"{PROCESSED_DIR}/velocity_heatmap_q1_2025.csv"),
        'tips': pd.read_csv(f"{PROCESSED_DIR}/tip_crowding_monthly.csv"),
        'rain': pd.read_csv(f"{PROCESSED_DIR}/rain_elasticity_2025.csv"),
        'audit_vendors': pd.read_csv(f"{DATA_DIR}/audit_logs/suspicious_vendors.csv")
    }

data = load_data()

# Custom plotly theme
plotly_theme = {
    'layout': {
        'paper_bgcolor': 'rgba(26, 26, 46, 0.6)',
        'plot_bgcolor': 'rgba(15, 15, 30, 0.8)',
        'font': {'color': '#e8e8e8', 'family': 'Work Sans, sans-serif'},
        'title': {
            'font': {'size': 20, 'family': 'Space Mono, monospace', 'color': '#ffd700'},
            'x': 0.05
        },
        'xaxis': {
            'gridcolor': 'rgba(255, 215, 0, 0.1)',
            'linecolor': 'rgba(255, 215, 0, 0.3)',
        },
        'yaxis': {
            'gridcolor': 'rgba(255, 215, 0, 0.1)',
            'linecolor': 'rgba(255, 215, 0, 0.3)',
        },
        'colorway': ['#ffd700', '#ff6b6b', '#00ff88', '#00d4ff', '#a388ff', '#ff88dc']
    }
}

# TABS
tab1, tab2, tab3, tab4 = st.tabs([
    " CONGESTION IMPACT", 
    " SPATIAL AUDIT", 
    " FAIRNESS INDEX", 
    " WEATHER ELASTICITY"
])

with tab1:
    st.markdown("<h3>Zone Entry Compliance & Volume Shift</h3>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'compliance_rate' in data['leakage'].columns:
            compliance_rate = data['leakage']['compliance_rate'].iloc[0]
            st.metric("COMPLIANCE RATE", f"{compliance_rate:.1%}", 
                     delta=f"{(compliance_rate - 0.85):.1%} vs target",
                     delta_color="normal")
        else:
            st.metric("COMPLIANCE RATE", "N/A", 
                     delta="data unavailable")
    
    with col2:
        # Try different possible column names for leakage
        leakage_col = None
        for col in ['suspected_leakage_trips', 'leakage_trips', 'trips', 'count']:
            if col in data['top_leakage'].columns:
                leakage_col = col
                break
        
        if leakage_col:
            total_leakage = data['top_leakage'][leakage_col].sum()
            st.metric("LEAKAGE INCIDENTS", f"{total_leakage:,.0f}",
                     delta="flagged for review",
                     delta_color="inverse")
        else:
            total_records = len(data['top_leakage'])
            st.metric("LEAKAGE RECORDS", f"{total_records:,.0f}",
                     delta="locations tracked",
                     delta_color="inverse")
    
    with col3:
        if 'trips_into_zone' in data['q1_volumes'].columns and len(data['q1_volumes']) >= 2:
            q1_change = ((data['q1_volumes']['trips_into_zone'].iloc[-1] / 
                         data['q1_volumes']['trips_into_zone'].iloc[0]) - 1)
            st.metric("Q1 VOLUME CHANGE", f"{q1_change:+.1%}",
                     delta="year-over-year",
                     delta_color="normal")
        else:
            st.metric("Q1 VOLUME CHANGE", "N/A",
                     delta="data unavailable")
    
    st.markdown("---")
    
    st.markdown("###   QUARTERLY ZONE ENTRY TRENDS")
    fig_q1 = px.bar(
            data['q1_volumes'], 
            x='quarter_start', 
            y='trips_into_zone', 
            color='taxi_type',
            barmode='group',
            title="Q1 2024 vs Q1 2025: Zone Entry Comparison"
        )
    fig_q1.update_layout(**plotly_theme['layout'])
    fig_q1.update_traces(marker_line_width=0)
    st.plotly_chart(fig_q1, use_container_width=True)

with tab2:
    st.markdown("###   Spatial Behavior Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Try to find dropoff column
        dropoff_col = None
        for col in ['dropoffs_near_border', 'dropoffs', 'trips', 'count']:
            if col in data['border'].columns:
                dropoff_col = col
                break
        
        if dropoff_col:
            border_metric = data['border'][dropoff_col].sum()
            st.metric("BORDER ZONE DROPOFFS", f"{border_metric:,.0f}",
                     delta="within 200m of boundary",
                     delta_color="inverse")
        else:
            border_metric = len(data['border'])
            st.metric("BORDER LOCATIONS", f"{border_metric:,.0f}",
                     delta="tracked zones",
                     delta_color="normal")
    
    st.markdown("---")
    
    st.markdown("###   BORDER EFFECT:")
    st.markdown("""
    <p style="color: #c8c8c8; margin-bottom: 1.5rem;">
    Analysis of dropoff patterns within 200m of the congestion zone boundary reveals potential toll avoidance behavior.
    </p>
    """, unsafe_allow_html=True)
    
    # Drop pct_change column
    border_df = data['border'].drop(columns=['pct_change'], errors='ignore')

    # Styled table (matches dark theme)
    border_styled = (
        border_df
        .style
        .format({
            "dropoffs": "{:,}"
        })
        .set_properties(**{
            "background-color": "rgba(255, 255, 255, 0.06)",
            "color": "#e8e8e8",
            "border": "1px solid rgba(255, 215, 0, 0.15)",
            "font-family": "Work Sans, sans-serif",
            "font-size": "0.9rem"
        })
        .set_table_styles([
            {
                "selector": "th",
                "props": [
                    ("background-color", "rgba(255, 215, 0, 0.15)"),
                    ("color", "#ffd700"),
                    ("font-weight", "700"),
                    ("text-transform", "uppercase"),
                    ("letter-spacing", "0.08em"),
                    ("border", "1px solid rgba(255, 215, 0, 0.3)")
                ]
            }
        ])
    )

    st.table(border_styled)

    
    st.markdown("---")
    st.markdown("###   VELOCITY HEATMAP COMPARISON")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<h3> Q1 2024 Baseline</h3>", unsafe_allow_html=True)

        if 'PULocationID' in data['velocity_24'].columns:
            pivot_24 = data['velocity_24'].pivot_table(
                index='PULocationID',
                columns='hour',
                values='avg_speed',
                aggfunc='mean'
            )

            fig_hm24 = px.imshow(
                pivot_24,
                labels=dict(
                    x="Hour of Day (0–23)",
                    y="Pickup Location ID (Taxi Zone)",
                    color="Average Speed (mph)"
                ),
                color_continuous_scale='RdYlGn',
                aspect="auto"
            )

        else:
            fig_hm24 = px.imshow(
                data['velocity_24'].corr(),
                color_continuous_scale='RdYlGn',
                title="Feature Correlation Matrix"
            )

        fig_hm24.update_layout(**plotly_theme['layout'])
        fig_hm24.update_layout(
            xaxis_title="Hour of Day (0–23)",
            yaxis_title="Pickup Location ID (Taxi Zone)"
        )

        st.plotly_chart(fig_hm24, use_container_width=True)

    with col2:
        st.markdown("<h3> Q1 2025 Post-Policy </h3>", unsafe_allow_html=True)

        if 'PULocationID' in data['velocity_25'].columns:
            pivot_25 = data['velocity_25'].pivot_table(
                index='PULocationID',
                columns='hour',
                values='avg_speed',
                aggfunc='mean'
            )

            fig_hm25 = px.imshow(
                pivot_25,
                labels=dict(
                    x="Hour of Day (0–23)",
                    y="Pickup Location ID (Taxi Zone)",
                    color="Average Speed (mph)"
                ),
                color_continuous_scale='RdYlGn',
                aspect="auto"
            )

        else:
            fig_hm25 = px.imshow(
                data['velocity_25'].corr(),
                color_continuous_scale='RdYlGn',
                title="Feature Correlation Matrix"
            )

        fig_hm25.update_layout(**plotly_theme['layout'])
        fig_hm25.update_layout(
            xaxis_title="Hour of Day (0–23)",
            yaxis_title="Pickup Location ID (Taxi Zone)"
        )

        st.plotly_chart(fig_hm25, use_container_width=True)

    st.markdown(
        "<p class='caption'>Heatmaps show average vehicle speed by taxi zone (Y-axis) and hour of day (X-axis). Color intensity reflects congestion severity.</p>",
        unsafe_allow_html=True
    )

with tab3:
    st.markdown("###  Economic Fairness Analysis")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'avg_surcharge' in data['tips'].columns:
            avg_surcharge = data['tips']['avg_surcharge'].mean()
            st.metric("AVG SURCHARGE", f"${avg_surcharge:.2f}",
                     delta="per trip",
                     delta_color="normal")
        else:
            st.metric("AVG SURCHARGE", "N/A",
                     delta="data unavailable")
    
    with col2:
        if 'tip_pct' in data['tips'].columns and len(data['tips']) >= 2:
            tip_decline = (data['tips']['tip_pct'].iloc[-1] - data['tips']['tip_pct'].iloc[0])
            st.metric("TIP RATE CHANGE", f"{tip_decline:+.1f}%",
                     delta="Jan → Mar 2025",
                     delta_color="inverse" if tip_decline < 0 else "normal")
        else:
            st.metric("TIP RATE CHANGE", "N/A",
                     delta="data unavailable")
    
    with col3:
        if 'avg_surcharge' in data['tips'].columns and 'total_trips' in data['tips'].columns:
            total_surcharge = (data['tips']['avg_surcharge'] * data['tips']['total_trips']).sum()
            st.metric("TOTAL SURCHARGES", f"${total_surcharge/1e6:.1f}M",
                     delta="Q1 2025 revenue",
                     delta_color="normal")
        else:
            records = len(data['tips'])
            st.metric("TIP RECORDS", f"{records}",
                     delta="months tracked",
                     delta_color="normal")
    
    st.markdown("---")
    st.markdown("###   SURCHARGE BURDEN vs DRIVER COMPENSATION")
    
    # Check if required columns exist
    has_month = 'month' in data['tips'].columns
    has_surcharge = 'avg_surcharge' in data['tips'].columns
    has_tip_pct = 'tip_pct' in data['tips'].columns
    
    if has_month and (has_surcharge or has_tip_pct):
        # Dual-axis chart
        fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
        
        if has_surcharge:
            fig_dual.add_trace(
                go.Bar(
                    x=data['tips']['month'], 
                    y=data['tips']['avg_surcharge'],
                    name="Avg Surcharge ($)",
                    marker_color='#ffd700',
                    marker_line_width=0
                ), 
                secondary_y=False
            )
        
        if has_tip_pct:
            fig_dual.add_trace(
                go.Scatter(
                    x=data['tips']['month'], 
                    y=data['tips']['tip_pct'],
                    name="Tip Percentage",
                    line=dict(color='#ff6b6b', width=4),
                    mode='lines+markers',
                    marker=dict(size=10, symbol='diamond')
                ), 
                secondary_y=True
            )
        
        fig_dual.update_xaxes(title_text="Month (2025)")
        if has_surcharge:
            fig_dual.update_yaxes(title_text="Average Surcharge ($)", secondary_y=False)
        if has_tip_pct:
            fig_dual.update_yaxes(title_text="Tip Percentage (%)", secondary_y=True)
        
        fig_dual.update_layout(**plotly_theme['layout'])
        fig_dual.update_layout(
            title="Dual Impact: Rising Costs vs Declining Tips",
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig_dual, use_container_width=True)
        
        st.markdown("""
        <p class="caption">
        <strong>Key Insight:</strong> As surcharges increase passenger costs, tip percentages decline, 
        creating a "double squeeze" on driver earnings. This suggests passengers may be mentally 
        offsetting tolls against gratuities.
        </p>
        """, unsafe_allow_html=True)
    else:
        st.warning(" Tip and surcharge data columns not found. Displaying available data:")
        st.dataframe(data['tips'], use_container_width=True, hide_index=True)

with tab4:
    st.markdown("###  Weather Demand Elasticity")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'elasticity_corr' in data['rain'].columns:
            elasticity = data['rain']['elasticity_corr'].iloc[0]
            st.metric("RAIN ELASTICITY", f"{elasticity:.3f}",
                     delta="correlation coefficient",
                     delta_color="normal")
        else:
            st.metric("RAIN ELASTICITY", "N/A",
                     delta="data unavailable")
    
    with col2:
        if 'prcp_mm' in data['rain'].columns:
            rainy_days = (data['rain']['prcp_mm'] > 0).sum()
            st.metric("RAINY DAYS", f"{rainy_days}",
                     delta="in Q1 2025",
                     delta_color="normal")
        else:
            records = len(data['rain'])
            st.metric("WEATHER RECORDS", f"{records}",
                     delta="days tracked",
                     delta_color="normal")
    
    with col3:
        if 'prcp_mm' in data['rain'].columns and 'daily_trips' in data['rain'].columns:
            rainy_avg = data['rain'][data['rain']['prcp_mm'] > 5]['daily_trips'].mean()
            dry_avg = data['rain'][data['rain']['prcp_mm'] == 0]['daily_trips'].mean()
            if dry_avg > 0:
                rain_premium = (rainy_avg / dry_avg) - 1
                st.metric("RAIN SURGE", f"{rain_premium:+.1%}",
                         delta="demand vs dry days",
                         delta_color="normal")
            else:
                st.metric("RAIN SURGE", "N/A",
                         delta="insufficient data")
        else:
            st.metric("RAIN SURGE", "N/A",
                     delta="data unavailable")
    
    st.markdown("---")
    st.markdown("###   PRECIPITATION vs TRIP DEMAND")
    
    if 'prcp_mm' in data['rain'].columns and 'daily_trips' in data['rain'].columns:
        # Try to add trendline, but don't fail if statsmodels isn't installed
        try:
            fig_rain = px.scatter(
                data['rain'], 
                x='prcp_mm', 
                y='daily_trips',
                trendline="ols",
                title="Daily Trip Volume as Function of Rainfall (Q1 2025)",
                labels={
                    'prcp_mm': 'Precipitation (mm)',
                    'daily_trips': 'Total Daily Trips'
                }
            )
        except (ModuleNotFoundError, ImportError):
            # Fallback without trendline if statsmodels not available
            fig_rain = px.scatter(
                data['rain'], 
                x='prcp_mm', 
                y='daily_trips',
                title="Daily Trip Volume as Function of Rainfall (Q1 2025)",
                labels={
                    'prcp_mm': 'Precipitation (mm)',
                    'daily_trips': 'Total Daily Trips'
                }
            )
        
        fig_rain.update_traces(
            marker=dict(
                size=12,
                color=data['rain']['prcp_mm'],
                colorscale='Blues',
                showscale=True,
                colorbar=dict(title="Rain (mm)"),
                line=dict(width=1, color='white')
            )
        )
        
        fig_rain.update_layout(**plotly_theme['layout'])
        st.plotly_chart(fig_rain, use_container_width=True)
    else:
        st.warning(" Precipitation or trip data not found. Displaying available data:")
        st.dataframe(data['rain'].head(20), use_container_width=True, hide_index=True)

# FOOTER
st.markdown("---")
st.markdown("""
    <div class="footer-section">
        <p class="caption">
        Data Sources:    TLC Trip Records · Data.Gov · Open Meteo
        </p>
        <p class="caption">
        Developed by Tooba Nadeem
        </p>
    </div>
""", unsafe_allow_html=True)