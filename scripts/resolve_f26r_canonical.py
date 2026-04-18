import pandas as pd
import numpy as np
import os
import re
from scipy.spatial import KDTree

def clean_tokens(text):
    if not isinstance(text, str): return []
    t = text.upper()
    # Broad cleanup of common functional words
    t = re.sub(r'\b(WELL|PAD|UNIT|IMPOUNDMENT|STATION|COMPRESSOR|ROAD|RD|TRACT|COP|ORIGIN|GENERATOR|SITE|FACILITY|LOC|LOCATION|WELLPAD)\b', ' ', t)
    t = re.sub(r'\d+', ' ', t)
    t = re.sub(r'[^A-Z ]', ' ', t)
    tokens = [tok for tok in t.split() if len(tok) > 2]
    return list(set(tokens))

def resolve_canonical():
    print("--- Canonical F26R Origin Resolution ---")
    
    # 1. Load Datasets
    print("Loading datasets...")
    mfl = pd.read_parquet('data/processed/master_facility_list_final.parquet')
    origins = pd.read_parquet('data/interim/f26r_origins.parquet')
    cer = pd.read_csv('data/raw/cer_lookup.csv')
    
    # Load geocoded cache
    geo_cache_path = 'data/interim/geocode_cache_origins.parquet'
    if os.path.exists(geo_cache_path):
        geo_cache = pd.read_parquet(geo_cache_path)
        origins = origins.merge(geo_cache, left_on='origin_addr', right_on='address', how='left')
    else:
        origins['lat_geo'] = None
        origins['lon_geo'] = None

    # Final Coordinate Logic: Priority 1: Extracted directly, Priority 2: Geocoded from address
    origins['lat_final'] = origins['origin_lat'].fillna(origins['lat_geo'])
    origins['lon_final'] = origins['origin_lon'].fillna(origins['lon_geo'])
    
    # 2. Map Parent_IDs
    cer_map = cer[['Raw_Name', 'Parent_ID']].drop_duplicates('Raw_Name')
    cer_map['Raw_Name'] = cer_map['Raw_Name'].str.strip().str.upper()
    origins['company_clean'] = origins['company_name'].str.strip().str.upper()
    origins = origins.merge(cer_map, left_on='company_clean', right_on='Raw_Name', how='left')
    
    # 3. Tokens
    origins['tokens'] = origins['origin_name'].apply(clean_tokens)
    
    # 4. Spatial Index
    mfl_valid = mfl.dropna(subset=['Centroid_Lat', 'Centroid_Lon'])
    tree = KDTree(mfl_valid[['Centroid_Lat', 'Centroid_Lon']].values)
    
    mfl_by_parent = {}
    for pid, group in mfl.groupby('Parent_ID'):
        mfl_by_parent[pid] = group.to_dict('records')

    # 5. Resolve
    print("Linking origins to MFL...")
    results = []
    
    for idx, row in origins.iterrows():
        pid = row['Parent_ID']
        f_tokens = set(row['tokens'])
        lat, lon = row['lat_final'], row['lon_final']
        
        best_score = 0
        best_match = None
        match_type = None

        # PASS 1: Strict Parent + Spatial (High Confidence)
        if pd.notna(lat) and pd.notna(pid) and pid in mfl_by_parent:
            candidates = mfl_by_parent[pid]
            for cand in candidates:
                dist = np.sqrt((lat - cand['Centroid_Lat'])**2 + (lon - cand['Centroid_Lon'])**2)
                if dist < 0.005: # ~500m
                    intersect = f_tokens & set(cand['Name_Tokens'])
                    score = 70 + (len(intersect) * 10)
                    if score > best_score:
                        best_score = score
                        best_match = cand['Master_Facility_ID']
                        match_type = 'Pass 1: Parent+Spatial'

        # PASS 2: Spatial Only (Relaxed Parent)
        if best_score < 70 and pd.notna(lat):
            nearby = tree.query_ball_point([lat, lon], 0.005)
            for m_idx in nearby:
                cand = mfl_valid.iloc[m_idx]
                intersect = f_tokens & set(cand['Name_Tokens'])
                score = 50 + (len(intersect) * 15)
                if score > best_score:
                    best_score = score
                    best_match = cand['Master_Facility_ID']
                    match_type = 'Pass 2: Spatial Only'

        # PASS 3: Parent + Name Only (No coords)
        if best_score < 40 and f_tokens and pd.notna(pid) and pid in mfl_by_parent:
            candidates = mfl_by_parent[pid]
            for cand in candidates:
                intersect = f_tokens & set(cand['Name_Tokens'])
                if not intersect: continue
                # Higher threshold for name only
                score = (len(intersect) / len(f_tokens)) * 100
                if score > best_score:
                    best_score = score
                    best_match = cand['Master_Facility_ID']
                    match_type = 'Pass 3: Parent+Name'

        if best_match and best_score >= 40:
            results.append({
                'filename': row['filename'],
                'Master_Facility_ID': best_match,
                'Confidence': min(best_score/100.0, 1.0),
                'Match_Method': match_type
            })

    df_res = pd.DataFrame(results)
    print(f" - Linked {len(df_res)} out of {len(origins)} unique files.")
    if not df_res.empty:
        print(df_res['Match_Method'].value_counts())
    
    # Save
    os.makedirs('data/processed', exist_ok=True)
    df_res.to_parquet('data/processed/f26r_origin_links.parquet')
    print(" - Saved to data/processed/f26r_origin_links.parquet")

if __name__ == "__main__":
    resolve_canonical()
