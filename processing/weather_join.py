# processing/weather_join.py
import requests
import pandas as pd
import polars as pl
import numpy as np
from config import PROCESSED_DIR, DATA_DIR
import os
import duckdb
from datetime import datetime
import glob

def fetch_central_park_weather():
    """Fetch 2025 daily PRCP for Central Park (40.7789°N, 73.9692°W)"""
    print("🌧️ Fetching Central Park 2025 weather...")
    
    # Open-Meteo API (free, no key)
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': 40.7789,    # Central Park
        'longitude': -73.9692,
        'start_date': '2025-01-01',
        'end_date': '2025-12-31',
        'daily': 'precipitation_sum'
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    weather_df = pd.DataFrame({
        'date': pd.to_datetime(data['daily']['time']),
        'prcp_mm': data['daily']['precipitation_sum']
    })

    weather_path = os.path.join(PROCESSED_DIR, 'central_park_weather_2025.csv')
    weather_df.to_csv(weather_path, index=False)
    print(f"✅ Weather saved: {len(weather_df)} days")
    return weather_df

def calculate_rain_elasticity():
    """Join taxi trips + weather → elasticity + wettest month plot + audit logs"""
    print("📊 Calculating Rain Elasticity...")
    
    clean_path = os.path.join(PROCESSED_DIR, "clean_taxi_2023_2025.parquet")
    weather_path = os.path.join(PROCESSED_DIR, "central_park_weather_2025.csv")
    
    con = duckdb.connect(':memory:')
    
    # Main taxi data (2025 only)
    con.execute(f"CREATE TABLE trips AS SELECT * FROM read_parquet('{clean_path}')")
    
    # Daily weather join
    con.execute(f"CREATE TABLE weather AS SELECT * FROM read_csv('{weather_path}')")
    
    elasticity_df = con.execute("""
        SELECT 
            w.date,
            w.prcp_mm,
            COUNT(t.*) AS daily_trips,
            AVG(t.trip_distance) AS avg_distance
        FROM weather w
        LEFT JOIN trips t ON CAST(t.pickup_time AS DATE) = w.date
        WHERE YEAR(w.date) = 2025
        GROUP BY 1, 2
        ORDER BY 1
    """).fetchdf()
    
    # Calculate correlation (Pandas)
    elasticity_corr = elasticity_df['prcp_mm'].corr(elasticity_df['daily_trips'])
    elasticity_df['elasticity_corr'] = elasticity_corr
    
    # Wettest month
    monthly_rain = elasticity_df.groupby(elasticity_df['date'].dt.month).agg({
        'prcp_mm': 'sum',
        'daily_trips': 'mean'
    }).sort_values('prcp_mm', ascending=False)
    
    wettest_month = monthly_rain.index[0]
    print(f"🌧️ Wettest month 2025: {wettest_month} ({monthly_rain['prcp_mm'].iloc[0]:.1f}mm)")
    
    # Save results
    elasticity_df.to_csv(os.path.join(PROCESSED_DIR, "rain_elasticity_2025.csv"), index=False)
    monthly_rain.to_csv(os.path.join(PROCESSED_DIR, "monthly_rain_2025.csv"))
    
    print(f"✅ Elasticity: {elasticity_corr:.3f}")
    
    # AUDIT LOGS - Populate audit_logs/ folder
    print("🔍 Generating audit logs...")
    
    # 1. Suspicious vendors (ghost trips)
    suspect_vendors = con.execute("""
        SELECT 
            CASE WHEN taxi_type = 'yellow' THEN 1 ELSE 2 END as VendorID,
            COUNT(*) as ghost_count
        FROM trips 
        WHERE avg_speed_mph > 50 OR trip_minutes < 1
          AND YEAR(pickup_time) = 2025
        GROUP BY 1 
        ORDER BY 2 DESC 
        LIMIT 5
    """).fetchdf()
    suspect_vendors.to_csv(os.path.join(DATA_DIR, "audit_logs", "suspicious_vendors.csv"), index=False)
    
    # 2. High leakage zones (from Phase 2)
    leakage_zones = con.execute("""
        SELECT pickup_loc, COUNT(*) as leakage_trips
        FROM trips 
        WHERE pickup_time >= '2025-01-05'
          AND congestion_surcharge <= 0
          AND dropoff_loc IN (4,12,13,24,41,42,43,45,48,50,68,74,75,79,87,88,90,91,92,93,94,100,107,113,114,116,125,137,143,161,162,170,186,193,209,211,224,229,230,231,233,234,236,237,238,239)
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).fetchdf()
    leakage_zones.to_csv(os.path.join(DATA_DIR, "audit_logs", "leakage_zones.csv"), index=False)
    
    print("✅ Audit logs populated!")
    print("Files ready for dashboard Tab 4")

if __name__ == "__main__":
    fetch_central_park_weather()
    calculate_rain_elasticity()
    print("✅ Phase 4 COMPLETE!")
