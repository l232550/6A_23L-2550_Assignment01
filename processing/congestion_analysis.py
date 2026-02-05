import duckdb
import pandas as pd
import polars as pl
import os
from config import PROCESSED_DIR, DATA_DIR

def load_congestion_zone_ids():
    """Get Manhattan LocationIDs (congestion zone approx)"""
    zones_path = os.path.join(DATA_DIR, "raw", "taxi_zones.csv")
    if not os.path.exists(zones_path):
        raise FileNotFoundError(f"Download taxi_zones.csv to {zones_path}")
    
    zones = pl.read_csv(zones_path)
    manhattan_ids = zones.filter(pl.col("Borough") == "Manhattan").select("Location ID").to_series().to_list()
    print(f"🏢 Congestion zone: {len(manhattan_ids)} Manhattan LocationIDs")
    return manhattan_ids

def run_congestion_analysis():
    """Phase 2 COMPLETE: leakage + Q1 2024 VS Q1 2025"""
    clean_path = os.path.join(PROCESSED_DIR, "clean_taxi_2023_2025.parquet")
    if not os.path.exists(clean_path):
        raise FileNotFoundError("Run Phase 1 first: python -m cleaning.ghost_filters")

    zone_ids = load_congestion_zone_ids()
    zone_ids_str = ",".join(map(str, zone_ids))

    con = duckdb.connect(':memory:')

    # Load ALL years (2023-2025) - NO PANDAS FULL LOAD
    con.execute(f"CREATE TABLE trips AS SELECT * FROM read_parquet('{clean_path}')")

    print("🔍 Phase 2 Analysis...")

    # 1. LEAKAGE AUDIT (post-Jan 5 2025, outside→inside zone)
    leakage_df = con.execute(f"""
        SELECT
            COUNT(*) AS total_trips,
            SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) AS with_surcharge,
            1.0 * SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) / COUNT(*) AS compliance_rate
        FROM trips
        WHERE pickup_time >= '2025-01-05'
          AND pickup_loc NOT IN ({zone_ids_str})
          AND dropoff_loc IN ({zone_ids_str})
    """).fetchdf()

    # Top 3 pickup locations missing surcharge
    top_missing_df = con.execute(f"""
        SELECT
            pickup_loc,
            COUNT(*) AS trips,
            1.0 * SUM(CASE WHEN congestion_surcharge <= 0 THEN 1 ELSE 0 END) / COUNT(*) AS missing_rate
        FROM trips
        WHERE pickup_time >= '2025-01-05'
          AND pickup_loc NOT IN ({zone_ids_str})
          AND dropoff_loc IN ({zone_ids_str})
        GROUP BY pickup_loc
        HAVING COUNT(*) > 100
        ORDER BY missing_rate DESC
        LIMIT 3
    """).fetchdf()

    # 2. YELLOW vs GREEN Q1 2024 VS Q1 2025 (entering zone)
    q1_comparison_df = con.execute(f"""
        SELECT
            DATE_TRUNC('quarter', pickup_time)::DATE AS quarter_start,
            taxi_type,
            COUNT(*) AS trips_into_zone
        FROM trips
        WHERE pickup_time >= '2024-01-01'
          AND pickup_time < '2025-04-01'  -- Q1 2024 + Q1 2025
          AND pickup_loc NOT IN ({zone_ids_str})
          AND dropoff_loc IN ({zone_ids_str})
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchdf()

    # Save tiny CSVs for dashboard
    leakage_df.to_csv(os.path.join(PROCESSED_DIR, "leakage_audit.csv"), index=False)
    top_missing_df.to_csv(os.path.join(PROCESSED_DIR, "top_leakage_pickups.csv"), index=False)
    q1_comparison_df.to_csv(os.path.join(PROCESSED_DIR, "q1_2024_vs_2025.csv"), index=False)

    # Print results
    print(f"✅ LEAKAGE: {leakage_df['compliance_rate'].iloc[0]:.1%} compliance rate")
    print("🚨 TOP 3 leakage pickups:")
    print(top_missing_df)
    print("\n📉 Q1 Zone Entry Volumes:")
    print(q1_comparison_df)

    return {
        "leakage": leakage_df,
        "top_missing": top_missing_df,
        "q1_comparison": q1_comparison_df,  # 2024+2025
    }

if __name__ == "__main__":
    run_congestion_analysis()
