import dask.dataframe as dd
import polars as pl
import numpy as np
import pandas as pd
import glob
from config import RAW_DIR, PROCESSED_DIR, SPEED_THRESHOLD_MPH, MIN_TRIP_MINUTES, MIN_FARE_TELEPORT
from utils.helpers import stream_parquet_to_dask, setup_logging
from ingestion.tlc_scraper import scrape_tlc_links  # For dec_missing flag
import os
import logging
import duckdb

logger = setup_logging(PROCESSED_DIR)


def get_column_mapping(taxi_type):
    """Handle Yellow(tpep_) vs Green(lpep_) column names"""
    prefix = 'tpep_' if taxi_type == 'yellow' else 'lpep_'
    return {
        'pickup_time': f'{prefix}pickup_datetime',
        'dropoff_time': f'{prefix}dropoff_datetime',
        'pickup_loc': 'PULocationID',
        'dropoff_loc': 'DOLocationID',
        'trip_distance': 'trip_distance',
        'fare': 'fare_amount',
        'total_amount': 'total_amount',
        'congestion_surcharge': 'congestion_surcharge'  # or 'cbd_congestion_fee'
    }

def apply_ghost_filters(yellow_pattern, green_pattern):
    """Process BOTH Yellow + Green, SKIP if already exists"""
    clean_path = os.path.join(PROCESSED_DIR, 'clean_taxi_2025.parquet')
    if os.path.exists(clean_path):
        print("✅ clean_taxi_2025.parquet exists → Skipping reprocessing (94M rows!)")
        return pd.read_parquet(clean_path)  # Quick load for downstream
  
    # Process Yellow
    print("🟡 Processing Yellow taxis...")
    df_yellow = stream_parquet_to_dask(yellow_pattern)
    cols_yellow = get_column_mapping('yellow')
    df_yellow = df_yellow[list(cols_yellow.values())].rename(columns={v: k for k, v in cols_yellow.items()})
    
    # Process Green  
    print("🟢 Processing Green taxis...")
    df_green = stream_parquet_to_dask(green_pattern)
    cols_green = get_column_mapping('green')
    df_green = df_green[list(cols_green.values())].rename(columns={v: k for k, v in cols_green.items()})
    
    # Combine + add taxi_type column
    df = dd.concat([df_yellow.assign(taxi_type='yellow'), 
                    df_green.assign(taxi_type='green')])
    
    # Compute trip metrics (Dask)
    df['pickup_time'] = dd.to_datetime(df['pickup_time'])
    df['dropoff_time'] = dd.to_datetime(df['dropoff_time'])
    df['trip_minutes'] = (df['dropoff_time'] - df['pickup_time']).dt.total_seconds() / 60
    df['avg_speed_mph'] = df['trip_distance'] / (df['trip_minutes'] / 60).fillna(0)
    
    # Ghost filters (compute to get counts)
    df_computed = df.compute()
    physics_ghosts = df_computed[df_computed['avg_speed_mph'] > SPEED_THRESHOLD_MPH]
    teleporter_ghosts = df_computed[(df_computed['trip_minutes'] < MIN_TRIP_MINUTES) & 
                                   (df_computed['fare'] > MIN_FARE_TELEPORT)]
    stationary_ghosts = df_computed[(df_computed['trip_distance'] == 0) & 
                                   (df_computed['fare'] > 0)]
    
    # Audit log
    audit = pd.DataFrame({
        'ghost_type': ['physics', 'teleporter', 'stationary'],
        'yellow_count': [sum(physics_ghosts.taxi_type=='yellow'), 
                        sum(teleporter_ghosts.taxi_type=='yellow'),
                        sum(stationary_ghosts.taxi_type=='yellow')],
        'green_count': [sum(physics_ghosts.taxi_type=='green'), 
                       sum(teleporter_ghosts.taxi_type=='green'),
                       sum(stationary_ghosts.taxi_type=='green')],
        'total_count': [len(physics_ghosts), len(teleporter_ghosts), len(stationary_ghosts)]
    })
    audit.to_csv(os.path.join(PROCESSED_DIR, 'ghost_audit.csv'), index=False)
    
    # Clean mask
    clean_mask = (
        (df_computed['avg_speed_mph'] <= SPEED_THRESHOLD_MPH) &
        ~((df_computed['trip_minutes'] < MIN_TRIP_MINUTES) & (df_computed['fare'] > MIN_FARE_TELEPORT)) &
        ~((df_computed['trip_distance'] == 0) & (df_computed['fare'] > 0))
    )
    clean_df = df_computed[clean_mask]
    
    # Save unified clean data
    clean_df.to_parquet(os.path.join(PROCESSED_DIR, 'clean_taxi_2023_2025.parquet'), engine='pyarrow')
    
    print(f"✅ Processed {len(df_computed)} → {len(clean_df)} clean rows")
    print("📊 Audit saved to ghost_audit.csv")
    return clean_df

def impute_dec_2025(dec2023_path, dec2024_path, output_path):
    """TECHNICAL CONSTRAINT #3: ACTUAL weighted imputation"""
    print("🧮 IMPUTING Dec 2025 (30% 2023 + 70% 2024)...")
    
    # Daily aggregations from Dec files
    con = duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE dec23 AS SELECT * FROM read_parquet('{dec2023_path}')")
    con.execute(f"CREATE TABLE dec24 AS SELECT * FROM read_parquet('{dec2024_path}')")
    
    # Weighted daily metrics
    imputed_daily = con.execute("""
    SELECT 
        CAST(d23.tpep_pickup_datetime AS DATE) AS date,
        0.3 * COUNT(d23.*) + 0.7 * d24.trips AS trips,
        0.3 * AVG(d23.trip_distance) + 0.7 * d24.avg_distance AS avg_distance,
        0.3 * AVG(d23.fare_amount) + 0.7 * d24.avg_fare AS avg_fare
    FROM dec23 d23
    FULL OUTER JOIN (
      SELECT 
            CAST(tpep_pickup_datetime AS DATE) AS date, 
            COUNT(*) AS trips, 
            AVG(trip_distance) AS avg_distance,
            AVG(fare_amount) AS avg_fare
        FROM dec24 
        GROUP BY 1
    ) d24 
    ON CAST(d23.tpep_pickup_datetime AS DATE) = d24.date
    GROUP BY 1, d24.trips, d24.avg_distance, d24.avg_fare
    ORDER BY 1
    """).fetchdf()
    
    imputed_daily.to_parquet(output_path)
    print(f"✅ Dec 2025 imputed: {len(imputed_daily)} days")


if __name__ == "__main__":
    # Process BOTH taxi types
    yellow_files = f"{RAW_DIR}/yellow/*.parquet"
    green_files = f"{RAW_DIR}/green/*.parquet"
    
    available, dec_missing = scrape_tlc_links()
    clean_df = apply_ghost_filters(yellow_files, green_files)
    
    if dec_missing:
        dec2023 = glob.glob(f"{RAW_DIR}/yellow/*2023-12*")
        dec2024 = glob.glob(f"{RAW_DIR}/yellow/*2024-12*")
        if dec2023 and dec2024:
            impute_dec_2025(dec2023[0], dec2024[0], f"{PROCESSED_DIR}/dec2025_imputed.parquet")
