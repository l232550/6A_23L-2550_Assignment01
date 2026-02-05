import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
from config import TLC_BASE_URL, RAW_DIR, YEARS, TAXI_TYPES
from utils.helpers import download_file, setup_logging
import logging

# prepare logger to write download logs
logger = setup_logging(RAW_DIR)

# function to scrape TLC links
def scrape_tlc_links():
    """Scrape TLC for ALL 2025 + imputation years Yellow/Green"""
    response = requests.get(TLC_BASE_URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    available = {'yellow': {}, 'green': {}}  # {year-month: url}
    
    for a in soup.find_all('a', href=True):
        href = a['href']
        if any(year in href for year in YEARS) and any(taxi in href.lower() for taxi in TAXI_TYPES) and href.endswith('.parquet'):
            taxi_type = 'yellow' if 'yellow' in href.lower() else 'green'
            fname = os.path.basename(href)
            # Parse: yellow_tripdata_2025-01.parquet → 2025-01
            if 'tripdata_' in fname:
                year_month = fname.split('tripdata_')[-1].replace('.parquet', '')
                available[taxi_type][year_month] = urljoin(TLC_BASE_URL, href)
    
    # Log what we found
    logger.info(f"Yellow: {list(available['yellow'].keys())}")
    logger.info(f"Green: {list(available['green'].keys())}")
    
    # Technical Constraint #3: Check Dec 2025 missing
    dec_2025_missing = '2025-12' not in available['yellow']
    print(f"📊 Yellow: {len(available['yellow'])} months, Green: {len(available['green'])} months")
    if dec_2025_missing:
        print("⚠️  Dec 2025 Yellow missing → Will impute from 2023(30%) + 2024(70%)")
    
    return available, dec_2025_missing

def download_tlc_data():
    """Download Yellow + Green 2025 (+ imputation years)"""
    available, dec_missing = scrape_tlc_links()
    downloaded = []
    
    for taxi_type, months in available.items():
        taxi_dir = os.path.join(RAW_DIR, taxi_type)
        os.makedirs(taxi_dir, exist_ok=True)
        
        for year_month, url in months.items():
            fname = f"{taxi_type}_{year_month}.parquet"
            filepath = os.path.join(taxi_dir, fname)
            
            if not os.path.exists(filepath):
                print(f"⬇️  {taxi_type}/{fname}")
                download_file(url, filepath)
                logger.info(f"Downloaded {fname}")
            else:
                print(f"✅ Exists: {fname}")
            downloaded.append(filepath)
    
    return downloaded, dec_missing


if __name__ == "__main__":
    download_tlc_data()
    print("✅ Phase 1 Ingestion complete! Check data/raw/yellow/ and data/raw/green/")
