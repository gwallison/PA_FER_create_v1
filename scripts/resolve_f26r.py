import pandas as pd
import numpy as np
import os
import re
from scipy.spatial import KDTree

def clean_f26r_name(text):
    if not isinstance(text, str): return []
    # Remove coordinates if present
    t = re.sub(r'[34][0-9]\.\d{3,}\s*[,/ ]\s*-?[78][0-9]\.\d{3,}', ' ', text)
    t = t.upper()
    
    # Remove common boilerplate sentences
    boilerplate = [
        r'EMPTY CONTAINERS ARE A RESIDUAL WASTE.*',
        r'GLYCOLS / ANTIFREEZE ARE A RESIDUAL WASTE.*',
        r'THE PRODUCED BRINE WATER IS STORED.*',
        r'NORMAL OPERATION AND MAINTENANCE ACTIVITIES.*',
        r'GENERATED DURING NORMAL OPERATION.*',
        r'WASTE IS STORMWATER FROM DIRECT PRECIPITATION.*',
        r'WASTE (?:IS|ARE) (?:A )?RESIDUAL WASTE.*',
        r'LOCATED AT.*'
    ]
    for pattern in boilerplate:
        t = re.sub(pattern, ' ', t)

    # Broad cleanup of common functional words
    t = re.sub(r'\b(WELL|PAD|UNIT|IMPOUNDMENT|STATION|COMPRESSOR|ROAD|RD|TRACT|COP|ORIGIN|GENERATOR|SITE|FACILITY|LOC|LOCATION|WELLPAD)\b', ' ', t)
    t = re.sub(r'\d+', ' ', t)
    t = re.sub(r'[^A-Z ]', ' ', t)
    tokens = [tok for tok in t.split() if len(tok) > 2]
    return list(set(tokens))

def resolve_relaxed():
    print("--- Optimized F26R Facility Resolution (Multi-Pass) ---")
    
    # 1. Load Data
    print("Loading datasets...")
    mfl = pd.read_parquet('data/processed/master_facility_list.parquet')
    f26r = pd.read_parquet('data/interim/f26r_with_coords.parquet')
    cer = pd.read_csv('data/raw/cer_lookup.csv')

    # Load geocoded cache if exists
    cache_path = 'data/interim/geocode_cache_origins.parquet'
    if os.path.exists(cache_path):
        print("Loading geocoded coordinates...")
        geo_cache = pd.read_parquet(cache_path)
        
        def extract_address_local(text):
            if not isinstance(text, str): return None
            match = re.search(r'(\d+[\w\s]{5,}(?:Road|Rd|Lane|Ln|Drive|Dr|Boulevard|Blvd|Street|St|Ave|Avenue|Pike|Highway|Hwy|Rt|Route|Way|Court|Ct|Circle|Cir)[\w\s,]*\d{5})', text, re.IGNORECASE)
            if match: return match.group(1).strip()
            match = re.search(r'(\d+[\w\s]{5,}(?:Road|Rd|Lane|Ln|Drive|Dr|Boulevard|Blvd|Street|St|Ave|Avenue|Pike|Highway|Hwy|Rt|Route|Way|Court|Ct|Circle|Cir))', text, re.IGNORECASE)
            if match: return match.group(1).strip()
            return None

        f26r['clean_addr'] = f26r['waste_location'].apply(extract_address_local).fillna(f26r['address'])
        f26r = f26r.merge(geo_cache, left_on='clean_addr', right_on='address', how='left', suffixes=('', '_geo'))
        # Fill missing coords with geocoded ones
        f26r['latitude'] = f26r['latitude'].fillna(f26r['lat_geo'])
        f26r['longitude'] = f26r['longitude'].fillna(f26r['lon_geo'])
        # Mark source
        f26r['coord_source'] = np.where(f26r['lat_geo'].notna() & f26r['coord_source'].isna(), 'geocoded', f26r['coord_source'])

    # 2. Map Parent_ID to F26R
    print("Linking F26R to CER Parent_IDs...")
    f26r['company_clean'] = f26r['company_name'].str.strip().str.upper()
    cer_map = cer[['Raw_Name', 'Parent_ID']].drop_duplicates('Raw_Name')
    cer_map['Raw_Name'] = cer_map['Raw_Name'].str.strip().str.upper()
    
    f26r = pd.merge(f26r, cer_map, left_on='company_clean', right_on='Raw_Name', how='left')

    # 3. Extract tokens from waste_location
    print("Extracting name tokens from F26R...")
    f26r['f26r_tokens'] = f26r['waste_location'].apply(clean_f26r_name)

    # 4. Prepare Spatial Index
    print("Building KDTree for spatial lookup...")
    # Filter MFL for records with coords (should be all, but safety first)
    mfl_valid = mfl.dropna(subset=['Centroid_Lat', 'Centroid_Lon'])
    mfl_coords = mfl_valid[['Centroid_Lat', 'Centroid_Lon']].values
    tree = KDTree(mfl_coords)
    mfl_valid_ids = mfl_valid.index.tolist()

    # 5. Optimized Pass 1 (Strict) Lookup
    mfl_by_parent = {}
    for pid, group in mfl.groupby('Parent_ID'):
        mfl_by_parent[pid] = group.to_dict('records')

    print("Executing Multi-Pass Probabilistic Linkage...")
    results = []
    total_f26r = len(f26r)
    processed = 0

    for idx, row in f26r.iterrows():
        processed += 1
        if processed % 5000 == 0:
            print(f" - Processed {processed}/{total_f26r}...")

        pid = row['Parent_ID']
        f_tokens = set(row['f26r_tokens'])
        lat, lon = row['latitude'], row['longitude']
        
        if not f_tokens and pd.isna(lat):
            continue
            
        best_score = 0
        best_match = None
        match_type = None

        # --- PASS 1: Strict Operator Blocking ---
        if pd.notna(pid) and pid in mfl_by_parent:
            candidates = mfl_by_parent[pid]
            for cand in candidates:
                intersect = f_tokens & set(cand['Name_Tokens'])
                if not intersect and pd.isna(lat): continue
                
                score = (len(intersect) / len(f_tokens)) * 100 if f_tokens else 0
                if pd.notna(lat):
                    dist = np.sqrt((lat - cand['Centroid_Lat'])**2 + (lon - cand['Centroid_Lon'])**2)
                    if dist < 0.005: score += 50
                
                if score > best_score:
                    best_score = score
                    best_match = cand['Master_Facility_ID']
                    match_type = 'Pass 1: Strict'

        # --- PASS 2: Relaxed Spatial (Ignore Operator) ---
        if best_score < 70 and pd.notna(lat):
            nearby = tree.query_ball_point([lat, lon], 0.005) # ~500m
            for m_idx in nearby:
                cand = mfl_valid.iloc[m_idx]
                intersect = f_tokens & set(cand['Name_Tokens'])
                
                dist = np.sqrt((lat - cand['Centroid_Lat'])**2 + (lon - cand['Centroid_Lon'])**2)
                spatial_score = (1 - (dist / 0.005)) * 100
                name_score = (len(intersect) / len(f_tokens)) * 100 if f_tokens else 0
                
                # Weight spatial proximity heavily in relaxed pass
                total_score = spatial_score * 0.8 + name_score * 0.2
                
                if total_score > best_score:
                    best_score = total_score
                    best_match = cand['Master_Facility_ID']
                    match_type = 'Pass 2: Relaxed Spatial'

        # --- PASS 3: High-Confidence Name Only (Within Parent) ---
        # For records without coordinates, look for unique name matches
        if best_score < 40 and pd.isna(lat) and pd.notna(pid) and pid in mfl_by_parent:
            candidates = mfl_by_parent[pid]
            for cand in candidates:
                intersect = f_tokens & set(cand['Name_Tokens'])
                if not intersect: continue
                
                # Scoring: what % of the F26R tokens are in the candidate?
                # We want a high threshold here to avoid false positives on generic names
                score = (len(intersect) / len(f_tokens)) * 100
                
                if score > best_score:
                    best_score = score
                    best_match = cand['Master_Facility_ID']
                    match_type = 'Pass 3: Name Only'

        if best_match and best_score >= 40:
            results.append({
                'f26r_index': idx,
                'Master_Facility_ID': best_match,
                'Confidence': min(best_score / 100.0, 1.0),
                'Match_Method': match_type
            })

    df_results = pd.DataFrame(results)
    print(f" - Successfully linked {len(df_results)} F26R records.")
    if not df_results.empty:
        print(df_results['Match_Method'].value_counts())
    
    # Merge results back
    f26r_resolved = f26r.merge(df_results, left_index=True, right_on='f26r_index', how='left')
    
    return f26r_resolved

if __name__ == "__main__":
    resolved = resolve_relaxed()
    os.makedirs('data/processed', exist_ok=True)
    resolved.to_parquet('data/processed/f26r_resolved_relaxed.parquet')
    print(" - Output saved to data/processed/f26r_resolved_relaxed.parquet")
