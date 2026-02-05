import os

# Initialize base dirs and data paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
AUDIT_DIR = os.path.join(DATA_DIR, "audit_logs")

# Create dirs
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(AUDIT_DIR, exist_ok=True)

# TLC Config - TECHNICAL CONSTRAINT #3: Multi-year for imputation
TLC_BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
YEARS = ["2023", "2024", "2025"]  # Need 2023/2024 Dec for imputation
TAXI_TYPES = ["yellow_tripdata", "green_tripdata"]
IMPUTE_WEIGHTS = {"2023": 0.3, "2024": 0.7}  # Dec missing → weighted avg

# Expected months (1-12)
EXPECTED_MONTHS = [f"{i:02d}" for i in range(1, 13)]

# Ghost filter thresholds
SPEED_THRESHOLD_MPH = 65      # > 65 MPH
MIN_TRIP_MINUTES = 1          # < 1 minute  
MIN_FARE_TELEPORT = 20       # > $20 (teleporter only)

