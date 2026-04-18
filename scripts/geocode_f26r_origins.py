import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import os
import time

def geocode_origins():
    print("--- F26R Origin Geocoding ---")
    origins = pd.read_parquet('data/interim/f26r_origins.parquet')
    
    # Get unique non-null addresses
    address_list = origins['origin_addr'].dropna().unique()
    print(f"Total unique addresses to check: {len(address_list)}")
    
    cache_path = 'data/interim/geocode_cache_origins.parquet'
    if os.path.exists(cache_path):
        cache = pd.read_parquet(cache_path)
    else:
        cache = pd.DataFrame(columns=['address', 'lat_geo', 'lon_geo'])
    
    to_process = [addr for addr in address_list if addr not in cache['address'].values]
    print(f"Addresses remaining to geocode: {len(to_process)}")
    
    if not to_process:
        print("Nothing to process.")
        return

    # Process in batches to allow for intermediate saves and rate limiting respect
    batch_size = 300
    to_process = to_process[:batch_size]
    print(f"Processing batch of {len(to_process)}...")

    geolocator = Nominatim(user_agent="pa_fer_origin_resolver_v1", timeout=10)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.2, error_wait_seconds=5)

    for addr in to_process:
        try:
            search_query = addr
            # Improve search query by ensuring PA context if missing
            if 'PA' not in addr.upper() and 'PENNSYLVANIA' not in addr.upper(): 
                search_query += ", Pennsylvania"
            if 'USA' not in addr.upper(): 
                search_query += ", USA"
            
            location = geocode(search_query)
            if location:
                row = pd.DataFrame([{'address': addr, 'lat_geo': location.latitude, 'lon_geo': location.longitude}])
                print(f" + Found: {addr[:40]} -> {location.latitude}, {location.longitude}")
            else:
                row = pd.DataFrame([{'address': addr, 'lat_geo': None, 'lon_geo': None}])
                print(f" - Not Found: {addr[:40]}")
            
            cache = pd.concat([cache, row]).drop_duplicates('address')
            cache.to_parquet(cache_path)
            
        except Exception as e:
            print(f"Error geocoding {addr}: {e}")

    print(f"Batch complete. Cache size: {len(cache)}")

if __name__ == "__main__":
    geocode_origins()
