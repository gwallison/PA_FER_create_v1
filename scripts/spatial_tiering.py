import pandas as pd
import numpy as np

def load_and_tier_data():
    """
    Main execution to load raw datasets and assign Spatial Tiers.
    """
    # 1. Process PA_wells (High Precision Baseline)
    # Using OilGasWellInventory.csv as the primary source for df_wells
    print("Loading PA_wells (OilGasWellInventory)...")
    try:
        df_wells = pd.read_csv('data/raw/OilGasWellInventory.csv')
        # All DEP inventory coordinates are considered Tier 1 (GPS/Surveyed)
        df_wells['Spatial_Tier'] = 'Tier 1'
        print(f" - Loaded {len(df_wells)} records as Tier 1")
    except Exception as e:
        print(f" - Error loading OilGasWellInventory: {e}")
        df_wells = pd.DataFrame()

    # 2. Process F26R (Mixed Precision)
    # Loading the harvested Form 26R data
    print("\nLoading F26R data...")
    try:
        df_f26r = pd.read_parquet('data/raw/all_harvested_form26r_v2.parquet')
        
        # Categorization Logic:
        # In Form 26R, coordinates often appear in 'waste_location' or 'address'.
        # If we have pre-existing lat/lon columns, we check their source.
        # For this script, we define the logic to be applied after/during geocoding.
        
        def assign_f26r_tier(row):
            # If coordinates were provided as Lat/Lon in the raw extraction
            # (e.g., extracted via regex from waste_location or facility_name)
            # we consider it Tier 1.
            
            # Placeholder for extraction logic check:
            # If 'waste_location' contains numeric patterns typical of Lat/Lon
            # or if a 'coord_source' flag indicates 'extracted'.
            
            if 'coord_source' in row and str(row['coord_source']).lower() in ['extracted', 'gps', 'provided']:
                return 'Tier 1'
            
            # If the coordinates come from the 'address' field via a geocoder
            return 'Tier 2'

        # If coordinates don't exist yet, we mark the records that WILL be geocoded
        # vs those that have extractable strings.
        df_f26r['Spatial_Tier'] = df_f26r.apply(assign_f26r_tier, axis=1)
        
        counts = df_f26r['Spatial_Tier'].value_counts()
        for tier, count in counts.items():
            print(f" - {tier}: {count} records")
            
    except Exception as e:
        print(f" - Error loading F26R: {e}")
        df_f26r = pd.DataFrame()

    return df_wells, df_f26r

if __name__ == "__main__":
    df_wells, df_f26r = load_and_tier_data()
    
    # Save interim results if needed
    # df_f26r.to_parquet('data/interim/f26r_tiered.parquet')
    print("\nSpatial Tiering analysis complete.")
