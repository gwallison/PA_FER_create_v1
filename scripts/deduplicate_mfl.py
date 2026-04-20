import pandas as pd
import numpy as np
from scipy.spatial import KDTree
import uuid
import os

def find_components(nodes, edges):
    """Simple BFS to find connected components without NetworkX."""
    adj = {node: set() for node in nodes}
    for u, v in edges:
        adj[u].add(v)
        adj[v].add(u)
        
    visited = set()
    components = []
    
    for node in nodes:
        if node not in visited:
            component = set()
            queue = [node]
            visited.add(node)
            while queue:
                u = queue.pop(0)
                component.add(u)
                for v in adj[u]:
                    if v not in visited:
                        visited.add(v)
                        queue.append(v)
            components.append(component)
    return components

def deduplicate_mfl(threshold_meters=150):
    print(f"--- Master Facility List Deduplication (Threshold: {threshold_meters}m) ---")
    
    # 1. Load Data
    mfl_path = 'data/processed/master_facility_list_final.parquet'
    if not os.path.exists(mfl_path):
        print("Error: Master Facility List not found.")
        return
    
    df = pd.read_parquet(mfl_path)
    print(f"Initial MFL size: {len(df)}")
    
    # 2. Identify candidate pairs for merging
    df_coords = df.dropna(subset=['Centroid_Lat', 'Centroid_Lon']).copy()
    coords = df_coords[['Centroid_Lat', 'Centroid_Lon']].values
    coords_m = coords * np.array([111000, 85000]) # Approx meters
    
    tree = KDTree(coords_m)
    pairs = tree.query_pairs(threshold_meters)
    
    edges = []
    print(f"Analyzing {len(pairs)} spatial candidate pairs...")
    
    for i, j in pairs:
        id_i = df_coords.iloc[i]['Master_Facility_ID']
        id_j = df_coords.iloc[j]['Master_Facility_ID']
        
        tokens_i = set(df_coords.iloc[i]['Name_Tokens'])
        tokens_j = set(df_coords.iloc[j]['Name_Tokens'])
        
        intersect = tokens_i & tokens_j
        
        # Site Match Criteria: Spatial proximity + Name overlap
        if len(intersect) > 0:
            edges.append((id_i, id_j))

    print(f"Found {len(edges)} high-confidence spatial+name matches.")
    
    # 3. Resolve Components as "Sites"
    components = find_components(df['Master_Facility_ID'].tolist(), edges)
    print(f"Identified {len(components)} distinct sites.")
    
    # Map Facility ID to Site ID
    id_to_site = {}
    for comp in components:
        site_id = "SITE-" + str(uuid.uuid4())[:8].upper()
        for fid in comp:
            id_to_site[fid] = site_id
            
    df['Site_ID'] = df['Master_Facility_ID'].map(id_to_site)
    
    # 4. Create Deduplicated MFL
    def aggregate_tokens(series):
        tokens = set()
        for t_list in series:
            if t_list is not None:
                tokens.update(t_list)
        return list(tokens)

    def aggregate_apis(series):
        apis = set()
        for a_list in series:
            if isinstance(a_list, (list, np.ndarray)):
                apis.update(a_list)
        return list(apis)

    print("Synthesizing Golden Site Records...")
    mfl_deduped = df.groupby('Site_ID').agg({
        'Parent_ID': lambda x: list(set(x.dropna())),
        'Centroid_Lat': 'mean',
        'Centroid_Lon': 'mean',
        'County': 'first',
        'Municipality': 'first',
        'Name_Tokens': aggregate_tokens,
        'Source': lambda x: list(set(x)),
        'Entity_Type': 'first',
        'Constituent_APIs': aggregate_apis,
        'Master_Facility_ID': lambda x: list(x)
    }).reset_index()
    
    # Clean up results
    mfl_deduped['Parent_Count'] = mfl_deduped['Parent_ID'].apply(len)
    mfl_deduped['Facility_Count'] = mfl_deduped['Master_Facility_ID'].apply(len)
    
    print(f"Deduplicated MFL size: {len(mfl_deduped)}")
    print(f" - Multi-operator sites: {len(mfl_deduped[mfl_deduped['Parent_Count'] > 1])}")
    print(f" - Multi-facility sites: {len(mfl_deduped[mfl_deduped['Facility_Count'] > 1])}")
    
    # 5. Save Outputs
    df.to_parquet('data/processed/master_facility_list_with_sites.parquet')
    mfl_deduped.to_parquet('data/processed/master_facility_list_deduped.parquet')
    
    print("\nDeduplication Complete.")
    print(" - Master List with Site IDs: data/processed/master_facility_list_with_sites.parquet")
    print(" - Deduplicated Golden Records: data/processed/master_facility_list_deduped.parquet")

if __name__ == "__main__":
    deduplicate_mfl()
