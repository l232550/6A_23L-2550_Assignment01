import pyarrow.parquet as pq
import dask.dataframe as dd
import polars as pl
from pathlib import Path
import logging

def setup_logging(log_dir):
    log_file = Path(log_dir) / "pipeline.log"
    logging.basicConfig(filename=log_file, level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def stream_parquet_to_dask(file_pattern):
    """Load parquet lazily without full memory load"""
    return dd.read_parquet(file_pattern, engine='pyarrow')

def download_file(url, filepath):
    import requests
    r = requests.get(url, stream=True)
    r.raise_for_status()
    with open(filepath, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
