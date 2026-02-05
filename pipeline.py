from config import RAW_DIR, PROCESSED_DIR
from ingestion.tlc_scraper import download_tlc_data
from cleaning.ghost_filters import apply_ghost_filters
from processing.congestion_analysis import run_congestion_analysis
from processing.visual_audit import run_visual_audit

def main():
    print("=== Phase 1: Ingestion + Cleaning ===")
    download_tlc_data()
    clean_path = os.path.join(PROCESSED_DIR, "clean_taxi_2025.parquet")
    if not os.path.exists(clean_path):
        apply_ghost_filters(
            yellow_pattern=f"{RAW_DIR}/yellow/*.parquet",
            green_pattern=f"{RAW_DIR}/green/*.parquet",
        )

    print("=== Phase 2: Congestion Zone Impact ===")
    run_congestion_analysis()

    print("=== Phase 3: Visual Audit ===")
    run_visual_audit()

if __name__ == "__main__":
    import os
    main()
