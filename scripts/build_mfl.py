import pandas as pd
import numpy as np
import os
from sklearn.cluster import AgglomerativeClustering
import uuid
import re

def clean_name_token(name):
    if not isinstance(name, str): return ""
    # Remove common suffixes and numbers
    n = name.upper()
    n = re.sub(r'\b(WELL|PAD|UNIT|IMPOUNDMENT|STATION|COMPRESSOR|ROAD|RD|TRACT|COP|WELLPAD)\b', ' ', n)
    n = re.sub(r'\d+', ' ', n) # Remove numbers
    n = re.sub(r'[^A-Z ]', ' ', n) # Remove punctuation
    tokens = [t for t in n.split() if len(t) > 2] # Only keep meaningful words
    return " ".join(tokens)

def load_and_preprocess():
    print("--- Phase 1: Data Integration & Pre-Processing ---")
    
    # 1. Load Data
    print("Loading OilGasWellInventory...")
    wells_path = 'data/raw/OilGasWellInventory.csv'
    if not os.path.exists(wells_path):
        print(f"Error: {wells_path} not found.")
        return None
    
    df_wells = pd.read_csv(wells_path, low_memory=False)
    
    print("Loading cer_lookup...")
    cer_path = 'data/raw/cer_lookup.csv'
    df_cer = pd.read_csv(cer_path)

    # 2. Filter Coordinates
    df_wells = df_wells.dropna(subset=['LATITUDE_DECIMAL', 'LONGITUDE_DECIMAL'])

    # 3. Map Parent_ID from CER
    df_wells['OPERATOR_CLEAN'] = df_wells['OPERATOR'].str.strip().str.upper()
    df_cer['Raw_Name_CLEAN'] = df_cer['Raw_Name'].str.strip().str.upper()
    cer_map = df_cer[['Raw_Name_CLEAN', 'Parent_ID']].drop_duplicates('Raw_Name_CLEAN')
    
    df_wells = pd.merge(df_wells, cer_map, left_on='OPERATOR_CLEAN', right_on='Raw_Name_CLEAN', how='left')
    df_wells = df_wells.drop(columns=['OPERATOR_CLEAN', 'Raw_Name_CLEAN'])
    
    # 4. Generate Clean Name Tokens for matching
    print("Cleaning name tokens (FARM and WELL_PAD)...")
    df_wells['clean_farm'] = df_wells['FARM'].apply(clean_name_token)
    df_wells['clean_pad_name'] = df_wells['WELL_PAD'].apply(clean_name_token)

    return df_wells

def cluster_wells(df_wells, distance_threshold_meters=400):
    print(f"\n--- Phase 2: Spatial Clustering (Threshold: {distance_threshold_meters}m) ---")
    
    df_wells['x'] = df_wells['LONGITUDE_DECIMAL'] * 85000 
    df_wells['y'] = df_wells['LATITUDE_DECIMAL'] * 111000
    df_wells['Master_Facility_ID'] = None
    
    # Handle existing WELL_PAD_ID
    has_pad = df_wells['WELL_PAD_ID'].notna()
    if has_pad.any():
        df_wells.loc[has_pad, 'Master_Facility_ID'] = df_wells[has_pad].apply(
            lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{x['Parent_ID']}_{int(x['WELL_PAD_ID'])}")), axis=1
        )

    # Cluster remaining
    needs_clustering = df_wells['Master_Facility_ID'].isna()
    blocks = df_wells[needs_clustering].groupby(['Parent_ID', 'COUNTY'])
    
    for (parent, county), group in blocks:
        if len(group) == 1:
            df_wells.loc[group.index, 'Master_Facility_ID'] = str(uuid.uuid4())
            continue

        clustering = AgglomerativeClustering(
            n_clusters=None,
            distance_threshold=distance_threshold_meters,
            linkage='complete' 
        ).fit(group[['x', 'y']])
        
        labels = clustering.labels_
        for label in np.unique(labels):
            u = str(uuid.uuid4())
            indices = group.index[labels == label]
            df_wells.loc[indices, 'Master_Facility_ID'] = u
            
    return df_wells

def generate_mfl(df_wells):
    print("\n--- Phase 3: Master Facility List Generation ---")
    
    # Aggregate attributes
    # We want a set of unique name tokens associated with each pad
    def aggregate_tokens(series):
        tokens = set()
        for s in series.dropna():
            tokens.update(s.split())
        return list(tokens)

    mfl = df_wells.groupby('Master_Facility_ID').agg({
        'Parent_ID': 'first',
        'LATITUDE_DECIMAL': 'mean',
        'LONGITUDE_DECIMAL': 'mean',
        'COUNTY': 'first',
        'MUNICIPALITY': 'first',
        'clean_farm': aggregate_tokens,
        'clean_pad_name': aggregate_tokens,
        'API': lambda x: list(x)
    }).reset_index()
    
    mfl.columns = ['Master_Facility_ID', 'Parent_ID', 'Centroid_Lat', 'Centroid_Lon', 'County', 'Municipality', 'Farm_Tokens', 'Pad_Tokens', 'Constituent_APIs']
    
    # Combine tokens for a single searchable list
    mfl['Name_Tokens'] = mfl.apply(lambda x: list(set(x['Farm_Tokens'] + x['Pad_Tokens'])), axis=1)
    
    print(f" - Final MFL contains {len(mfl)} facilities with rich name tokens.")
    return mfl

if __name__ == "__main__":
    df_wells = load_and_preprocess()
    if df_wells is not None:
        df_wells = cluster_wells(df_wells)
        mfl = generate_mfl(df_wells)
        
        os.makedirs('data/processed', exist_ok=True)
        mfl.to_parquet('data/processed/master_facility_list.parquet')
        df_wells.to_parquet('data/interim/wells_clustered.parquet')
        print(f" - MFL saved to data/processed/master_facility_list.parquet")

if __name__ == "__main__":
    df_wells = load_and_preprocess()
    if df_wells is not None:
        df_wells = cluster_wells(df_wells)
        mfl = generate_mfl(df_wells)
        
        os.makedirs('data/processed', exist_ok=True)
        mfl.to_parquet('data/processed/master_facility_list.parquet')
        df_wells.to_parquet('data/interim/wells_clustered.parquet')
        
        print(f"\nProcessing complete.")
        print(f" - MFL saved to data/processed/master_facility_list.parquet")
        print(f" - Clustered wells saved to data/interim/wells_clustered.parquet")
