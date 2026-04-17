import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import os
import re
import time

def extract_address(text):
    if not isinstance(text, str): return None
    match = re.search(r'(\d+[\w\s]{5,}(?:Road|Rd|Lane|Ln|Drive|Dr|Boulevard|Blvd|Street|St|Ave|Avenue|Pike|Highway|Hwy|Rt|Route|Way|Court|Ct|Circle|Cir)[\w\s,]*\d{5})', text, re.IGNORECASE)
    if match: return match.group(1).strip()
    match = re.search(r'(\d+[\w\s]{5,}(?:Road|Rd|Lane|Ln|Drive|Dr|Boulevard|Blvd|Street|St|Ave|Avenue|Pike|Highway|Hwy|Rt|Route|Way|Court|Ct|Circle|Cir))', text, re.IGNORECASE)
    if match: return match.group(1).strip()
    return None

def geocode_unmatched():
    print("--- F26R Geocoding (Batch 2) ---")
    f26r = pd.read_parquet('data/processed/f26r_resolved_relaxed.parquet')
    unmatched = f26r[f26r['Master_Facility_ID'].isna()].copy()
    unmatched['extracted_address'] = unmatched['waste_location'].apply(extract_address)
    unmatched['best_address'] = unmatched['extracted_address'].fillna(unmatched['address'])
    address_list = unmatched.dropna(subset=['best_address'])
    unique_addresses = address_list['best_address'].unique()
    
    cache_path = 'data/interim/geocode_cache.parquet'
    cache = pd.read_parquet(cache_path) if os.path.exists(cache_path) else pd.DataFrame(columns=['address', 'lat_geo', 'lon_geo'])
    
    to_process = [addr for addr in unique_addresses if addr not in cache['address'].values][:300]
    print(f"Geocoding {len(to_process)} new addresses...")

    geolocator = Nominatim(user_agent="pa_fer_resolver_v4", timeout=10)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.2, error_wait_seconds=5)

    for addr in to_process:
        try:
            search_query = addr
            if 'PA' not in addr.upper(): search_query += ", Pennsylvania"
            if 'USA' not in addr.upper(): search_query += ", USA"
            
            location = geocode(search_query)
            if location:
                row = pd.DataFrame([{'address': addr, 'lat_geo': location.latitude, 'lon_geo': location.longitude}])
            else:
                row = pd.DataFrame([{'address': addr, 'lat_geo': None, 'lon_geo': None}])
            
            cache = pd.concat([cache, row]).drop_duplicates('address')
            cache.to_parquet(cache_path)
            print(f" - Geocoded: {addr[:30]}...")
            
        except Exception as e:
            print(f"Error: {e}")

    print(f"Batch complete. Cache size: {len(cache)}")

if __name__ == "__main__":
    geocode_unmatched()
