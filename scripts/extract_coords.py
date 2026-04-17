import pandas as pd
import re
import os

def extract_lat_lon(text):
    if not isinstance(text, str):
        return None, None, None
    
    # Regex for decimal degrees
    # Pattern: 39-42 (lat) and -74 to -81 (lon)
    pattern = r'([34][0-9]\.\d{3,})\s*[,/ ]\s*(-[78][0-9]\.\d{3,})'
    match = re.search(pattern, text)
    if match:
        return float(match.group(1)), float(match.group(2)), 'extracted_regex'
    
    return None, None, None

def process_f26r_coords():
    print("Loading F26R data...")
    df = pd.read_parquet('data/raw/all_harvested_form26r_v2.parquet')
    
    print("Extracting coordinates from 'waste_location'...")
    results = df['waste_location'].apply(extract_lat_lon)
    
    df['latitude'] = [x[0] for x in results]
    df['longitude'] = [x[1] for x in results]
    df['coord_source'] = [x[2] for x in results]
    
    extracted_count = df['latitude'].notna().sum()
    print(f" - Successfully extracted {extracted_count} coordinates from raw text.")
    
    if extracted_count > 0:
        print("\nSample of extracted coords:")
        print(df[df['latitude'].notna()][['waste_location', 'latitude', 'longitude']].head(5))
        
    return df

if __name__ == "__main__":
    df_f26r = process_f26r_coords()
    os.makedirs('data/interim', exist_ok=True)
    df_f26r.to_parquet('data/interim/f26r_with_coords.parquet')
