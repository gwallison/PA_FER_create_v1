import pandas as pd
import numpy as np
import uuid
import os
import re

def synthesize():
    print("--- F26R Entity Synthesis & MFL Expansion ---")
    
    # 1. Load Data
    print("Loading data...")
    mfl = pd.read_parquet('data/processed/master_facility_list.parquet')
    f26r = pd.read_parquet('data/processed/f26r_resolved_relaxed.parquet')
    
    # Isolate unmatched records
    unmatched_mask = f26r['Master_Facility_ID'].isna()
    unmatched = f26r[unmatched_mask].copy()
    print(f"Total F26R records to synthesize: {len(unmatched)}")

    if len(unmatched) == 0:
        print("No unmatched records found. Synthesis skipped.")
        return

    # 2. Group by Parent_ID and waste_location string
    # We use the raw waste_location as a proxy for the facility name
    unmatched['synth_group_key'] = unmatched['Parent_ID'].fillna('Unknown') + "||" + unmatched['waste_location'].fillna('Unknown')
    
    synth_clusters = unmatched.groupby('synth_group_key')
    
    new_facilities = []
    print(f"Synthesizing {len(synth_clusters)} new master facility clusters...")
    
    for key, group in synth_clusters:
        # Generate New Master ID
        master_id = "SYN-" + str(uuid.uuid4())[:8].upper()
        
        # Determine attributes
        parent_id = group['Parent_ID'].iloc[0]
        # Use mean coords if any exist
        lat = group['latitude'].mean() if group['latitude'].notna().any() else None
        lon = group['longitude'].mean() if group['longitude'].notna().any() else None
        
        # Tag entity type based on keywords
        loc_text = str(group['waste_location'].iloc[0]).upper()
        entity_type = "Well Pad (F26R)"
        if "STATION" in loc_text or "COMPRESSOR" in loc_text:
            entity_type = "Compressor Station (F26R)"
        elif "IMPOUNDMENT" in loc_text:
            entity_type = "Impoundment (F26R)"
            
        new_facilities.append({
            'Master_Facility_ID': master_id,
            'Parent_ID': parent_id,
            'Centroid_Lat': lat,
            'Centroid_Lon': lon,
            'Entity_Type': entity_type,
            'Raw_Pad_Names': loc_text,
            'Source': 'F26R Synthesis',
            'Name_Tokens': list(set(group['f26r_tokens'].iloc[0])) if 'f26r_tokens' in group.columns else [],
            'County': group['bgCountyName'].iloc[0] if 'bgCountyName' in group.columns else None
        })
        
        # Update original F26R records
        # Use the index of the original dataframe
        f26r.loc[group.index, 'Master_Facility_ID'] = master_id
        f26r.loc[group.index, 'Match_Method'] = 'Synthesized'
        f26r.loc[group.index, 'Confidence'] = 0.90 # High local confidence, but marked as synth
        
    # 3. Expand MFL
    df_new_mfl = pd.DataFrame(new_facilities)
    
    # Standardize MFL columns
    if 'Source' not in mfl.columns: mfl['Source'] = 'DEP Inventory'
    if 'Entity_Type' not in mfl.columns: mfl['Entity_Type'] = 'Well Pad'
    
    combined_mfl = pd.concat([mfl, df_new_mfl], ignore_index=True)
    
    print(f" - Synthesized {len(df_new_mfl)} new entities.")
    print(f" - Final Master Facility List count: {len(combined_mfl)}")
    
    # 4. Save Final Outputs
    combined_mfl.to_parquet('data/processed/master_facility_list_final.parquet')
    f26r.to_parquet('data/processed/f26r_resolved_final.parquet')
    print("\nResolution Pipeline Complete.")
    print(" - Final MFL: data/processed/master_facility_list_final.parquet")
    print(" - Final F26R Linkage: data/processed/f26r_resolved_final.parquet")

if __name__ == "__main__":
    synthesize()
