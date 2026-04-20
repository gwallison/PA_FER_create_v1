import pandas as pd
import numpy as np
from scipy.spatial import KDTree

def find_near_duplicates(df, threshold_meters=500):
    # Drop NaNs
    df_clean = df.dropna(subset=['Centroid_Lat', 'Centroid_Lon']).copy()
    
    # Convert lat/lon to approximate meters (Mercator-ish)
    coords = df_clean[['Centroid_Lat', 'Centroid_Lon']].values
    coords_m = coords * np.array([111000, 85000])
    
    tree = KDTree(coords_m)
    pairs = tree.query_pairs(threshold_meters)
    
    results = []
    for i, j in pairs:
        row_i = df_clean.iloc[i]
        row_j = df_clean.iloc[j]
        
        # Calculate actual distance
        dist = np.sqrt(np.sum((coords_m[i] - coords_m[j])**2))
        
        results.append({
            'ID_1': row_i['Master_Facility_ID'],
            'ID_2': row_j['Master_Facility_ID'],
            'Parent_1': row_i['Parent_ID'],
            'Parent_2': row_j['Parent_ID'],
            'Dist': dist,
            'Name_1': row_i['Name_Tokens'],
            'Name_2': row_j['Name_Tokens'],
            'Source_1': row_i['Source'],
            'Source_2': row_j['Source']
        })
    
    return pd.DataFrame(results)

if __name__ == "__main__":
    df = pd.read_parquet('data/processed/master_facility_list_final.parquet')
    dupes = find_near_duplicates(df)
    print(f"Found {len(dupes)} near-duplicate pairs within 500m.")
    if len(dupes) > 0:
        diff_sources = dupes[dupes['Source_1'] != dupes['Source_2']]
        print(f"\n{len(diff_sources)} pairs have different Sources (DEP vs SYN).")
        if len(diff_sources) > 0:
            print(diff_sources.head(20).to_string())
        
        # Also check for SYN vs SYN duplicates
        syn_dupes = dupes[(dupes['Source_1'] == 'F26R Synthesis') & (dupes['Source_2'] == 'F26R Synthesis')]
        print(f"\n{len(syn_dupes)} SYN vs SYN pairs found within 500m.")
        if len(syn_dupes) > 0:
            print(syn_dupes.head(10).to_string())
