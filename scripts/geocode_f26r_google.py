import pandas as pd
import numpy as np
import os
import re
from geopy.geocoders import GoogleV3, ArcGIS
from geopy.extra.rate_limiter import RateLimiter
import time

def get_geocoder():
    google_key = os.environ.get('Google-maps-geocoding-api')
    if google_key:
        print("Using Google Maps Geocoder...")
        return GoogleV3(api_key=google_key)
    else:
        print("Google key not found. Using ArcGIS fallback...")
        return ArcGIS()

def geocode_google():
    print("--- F26R Origin Geocoding (Google/ArcGIS) ---")
    
    # 1. Load Data
    origins = pd.read_parquet('data/interim/f26r_origins.parquet')
    links = pd.read_parquet('data/processed/f26r_origin_links.parquet')
    cache_path = 'data/interim/geocode_cache_origins.parquet'
    
    if os.path.exists(cache_path):
        cache = pd.read_parquet(cache_path)
    else:
        cache = pd.DataFrame(columns=['address', 'lat_geo', 'lon_geo'])
    
    # 2. Identify unlinked addresses first
    unlinked_files = origins[~origins['filename'].isin(links['filename'])]
    unlinked_addrs = set(unlinked_files['origin_addr'].dropna().unique())
    all_addrs = set(origins['origin_addr'].dropna().unique())
    
    # 3. Identify what needs geocoding (missing or NaN)
    def needs_geo(addr):
        if addr not in cache['address'].values:
            return True
        val = cache[cache['address'] == addr]['lat_geo'].iloc[0]
        return pd.isna(val)
    
    to_process_unlinked = [a for a in unlinked_addrs if needs_geo(a)]
    to_process_other = [a for a in all_addrs if a not in unlinked_addrs and needs_geo(a)]
    
    to_process = to_process_unlinked + to_process_other
    print(f"Total addresses needing geocode: {len(to_process)} ({len(to_process_unlinked)} are unlinked)")
    
    if not to_process:
        print("All addresses geocoded.")
        return

    # 4. Initialize Geocoder
    geolocator = get_geocoder()
    # Google doesn't need much delay, but let's be safe
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=0.1)

    count = 0
    batch_size = 500 # Process in chunks to be safe
    
    for addr in to_process[:batch_size]:
        count += 1
        try:
            search_query = addr
            if 'PA' not in addr.upper() and 'PENNSYLVANIA' not in addr.upper(): 
                search_query += ", Pennsylvania"
            
            location = geocode(search_query)
            
            if location:
                # Update cache (remove old NaN entry if exists)
                cache = cache[cache['address'] != addr]
                new_row = pd.DataFrame([{'address': addr, 'lat_geo': location.latitude, 'lon_geo': location.longitude}])
                cache = pd.concat([cache, new_row], ignore_index=True)
                print(f"[{count}] Found: {addr[:50]} -> {location.latitude}, {location.longitude}")
            else:
                # If we didn't find it with Google, mark it so we don't try again this run
                # (Keep NaN if it was already NaN, or add it)
                if addr not in cache['address'].values:
                    new_row = pd.DataFrame([{'address': addr, 'lat_geo': None, 'lon_geo': None}])
                    cache = pd.concat([cache, new_row], ignore_index=True)
                print(f"[{count}] NOT Found: {addr[:50]}")
            
            # Save every 10 records
            if count % 10 == 0:
                cache.to_parquet(cache_path)
                
        except Exception as e:
            print(f"Error geocoding {addr}: {e}")
            time.sleep(2)

    # Final save
    cache.to_parquet(cache_path)
    print(f"Finished batch. Cache size: {len(cache)}")

if __name__ == "__main__":
    geocode_google()
