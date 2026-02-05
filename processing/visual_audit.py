import duckdb
import pandas as pd
import polars as pl
import numpy as np
from config import PROCESSED_DIR, DATA_DIR
import os

def run_visual_audit():
    """Phase 3: Generate viz-ready CSVs (Aggregation First!)"""
    clean_path = os.path.join(PROCESSED_DIR, "clean_taxi_2023_2025.parquet")
    zones_path = os.path.join(DATA_DIR, "raw", "taxi_zones.csv")
    
    con = duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE trips AS SELECT * FROM read_parquet('{clean_path}')")
    
    # Get Manhattan zones
    zones = pl.read_csv(zones_path)
    manhattan_ids = zones.filter(pl.col("Borough") == "Manhattan").select("Location ID").to_series().to_list()
    zone_ids_str = ",".join(map(str, manhattan_ids))
    
    print("🎨 Phase 3: Visual Audit Aggregations...")

    # 1. BORDER EFFECT: % change dropoffs in border zones (2024 vs 2025)
    # Border zones = just north of 60th: Upper East/West Side North (236,238)
    border_zones = [236, 237, 238, 239]  # Upper East/West North/South
    border_str = ",".join(map(str, border_zones))
    
    border_effect = con.execute(f"""
        SELECT 
            dropoff_loc,
            EXTRACT(YEAR FROM pickup_time) AS year,
            COUNT(*) AS dropoffs
        FROM trips 
        WHERE dropoff_loc IN ({border_str})
          AND pickup_time >= '2024-01-01'
          AND pickup_time < '2026-01-01'
        GROUP BY 1, 2
    """).fetchdf()
    
    # Calculate % change
    border_effect['pct_change'] = (
        (border_effect[border_effect.year == 2025]['dropoffs'] / 
         border_effect[border_effect.year == 2024]['dropoffs'] - 1) * 100
    ).fillna(0)
    
    border_effect.to_csv(os.path.join(PROCESSED_DIR, "border_effect.csv"), index=False)

    # 2. CONGESTION VELOCITY HEATMAP: Avg speed inside zone by hour+day
    velocity_heatmaps = con.execute(f"""
    SELECT 
        EXTRACT(DOW FROM pickup_time) AS day_of_week,
        EXTRACT(HOUR FROM pickup_time) AS hour_of_day,
        DATE_TRUNC('quarter', pickup_time)::DATE AS quarter,
        AVG(trip_distance / NULLIF(EXTRACT(EPOCH FROM (dropoff_time - pickup_time))/3600.0, 0)) AS avg_speed_mph,
        COUNT(*) AS trips
    FROM trips
    WHERE pickup_loc IN ({zone_ids_str}) 
        AND dropoff_loc IN ({zone_ids_str})
        AND pickup_time >= '2024-01-01'
        AND pickup_time < '2025-04-01'
    GROUP BY 1, 2, 3
    HAVING COUNT(*) > 10
    """).fetchdf()
    
    # Pivot for heatmaps
    q1_2024 = velocity_heatmaps[velocity_heatmaps.quarter == '2024-01-01'].pivot_table(
        index='day_of_week', columns='hour_of_day', values='avg_speed_mph', aggfunc='mean'
    ).fillna(15)  # Default speed
    
    q1_2025 = velocity_heatmaps[velocity_heatmaps.quarter == '2025-01-01'].pivot_table(
        index='day_of_week', columns='hour_of_day', values='avg_speed_mph', aggfunc='mean'
    ).fillna(15)
    
    q1_2024.to_csv(os.path.join(PROCESSED_DIR, "velocity_heatmap_q1_2024.csv"))
    q1_2025.to_csv(os.path.join(PROCESSED_DIR, "velocity_heatmap_q1_2025.csv"))

    # 3. TIP CROWDING: Monthly surcharge vs tip %
    tip_crowding = con.execute(f"""
        SELECT 
            DATE_TRUNC('month', pickup_time) AS month,
            AVG(congestion_surcharge) AS avg_surcharge,
            AVG((total_amount - fare - congestion_surcharge) / NULLIF(total_amount, 0)) AS tip_pct
        FROM trips
        WHERE pickup_time >= '2025-01-01'
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    
    tip_crowding.to_csv(os.path.join(PROCESSED_DIR, "tip_crowding_monthly.csv"), index=False)

    print("✅ Phase 3 COMPLETE - Viz files ready:")
    print("  - border_effect.csv (choropleth)")
    print("  - velocity_heatmap_q1_2024.csv (heatmap)")
    print("  - velocity_heatmap_q1_2025.csv (heatmap)") 
    print("  - tip_crowding_monthly.csv (dual-axis)")

if __name__ == "__main__":
    run_visual_audit()
