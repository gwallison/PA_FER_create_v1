import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import os
import numpy as np

st.set_page_config(layout="wide", page_title="F26R Link Auditor")

@st.cache_data
def load_audit_data():
    mfl = pd.read_parquet('data/processed/master_facility_list_final.parquet')
    origins = pd.read_parquet('data/interim/f26r_origins.parquet')
    links = pd.read_parquet('data/processed/f26r_origin_links.parquet')
    wells = pd.read_parquet('data/interim/wells_clustered.parquet', 
                            columns=['Master_Facility_ID', 'API', 'LATITUDE_DECIMAL', 'LONGITUDE_DECIMAL'])
    
    # Load geocoded cache to get the final coords used in resolution
    geo_cache_path = 'data/interim/geocode_cache_origins.parquet'
    if os.path.exists(geo_cache_path):
        geo_cache = pd.read_parquet(geo_cache_path)
        origins = origins.merge(geo_cache, left_on='origin_addr', right_on='address', how='left')
    else:
        origins['lat_geo'] = np.nan
        origins['lon_geo'] = np.nan

    origins['lat_final'] = origins['origin_lat'].fillna(origins['lat_geo'])
    origins['lon_final'] = origins['origin_lon'].fillna(origins['lon_geo'])

    # Join links with origin metadata
    full_df = links.merge(origins, on='filename', how='inner')
    full_df = full_df.merge(mfl[['Master_Facility_ID', 'Centroid_Lat', 'Centroid_Lon', 'Name_Tokens', 'County']], 
                               on='Master_Facility_ID', suffixes=('_f26r', '_mfl'))
    
    # Filter for spatial matches
    full_df = full_df[full_df['Match_Method'].isin(['Pass 1: Parent+Spatial', 'Pass 2: Spatial Only'])]

    # Filter out already audited files
    audit_path = 'data/processed/f26r_link_audit.parquet'
    if os.path.exists(audit_path):
        audited = pd.read_parquet(audit_path)
        full_df = full_df[~full_df['filename'].isin(audited['filename'])]

    if full_df.empty:
        return pd.DataFrame(), mfl, wells

    # Group by identical evidence to collapse duplicates
    # We group by the core visual attributes
    # Name_Tokens is a list (unhashable), so we aggregate it instead
    group_cols = ['waste_location', 'company_name', 'Master_Facility_ID', 
                  'lat_final', 'lon_final', 'Centroid_Lat', 'Centroid_Lon', 
                  'origin_name', 'origin_addr', 'set_name', 'page_number', 'County']
    
    audit_df = full_df.groupby(group_cols).agg({
        'filename': lambda x: list(x),
        'Match_Method': 'first',
        'Name_Tokens': 'first'
    }).reset_index()
    
    audit_df['file_count'] = audit_df['filename'].apply(len)
    
    # Calculate distance in meters (approx)
    audit_df['dist_m'] = np.sqrt((audit_df['lat_final'] - audit_df['Centroid_Lat'])**2 + 
                                 (audit_df['lon_final'] - audit_df['Centroid_Lon'])**2) * 111139
        
    return audit_df, mfl, wells

audit_df, mfl, wells = load_audit_data()

st.sidebar.title("Audit Controls")
if st.sidebar.button("Clear Cache"):
    st.cache_data.clear()
    st.rerun()

st.title("F26R Spatial Link Auditor")
st.write(f"Unique Audit Cases: {len(audit_df)} (Representing {audit_df['file_count'].sum() if not audit_df.empty else 0} files)")

if audit_df.empty:
    st.success("All spatial links audited!")
    st.stop()

# --- Selection ---
col1, col2 = st.columns([1, 3])

with col1:
    sort_by = st.selectbox("Sort By", ["Distance (High to Low)", "Distance (Low to High)", "File Count"])
    
    display_df = audit_df
    if "High to Low" in sort_by:
        display_df = display_df.sort_values('dist_m', ascending=False)
    elif "Low to High" in sort_by:
        display_df = display_df.sort_values('dist_m', ascending=True)
    elif "File Count" in sort_by:
        display_df = display_df.sort_values('file_count', ascending=False)
    
    selected_idx = st.selectbox("Select Case", display_df.index, 
                                format_func=lambda x: f"[{audit_df.loc[x, 'file_count']} files] {audit_df.loc[x, 'dist_m']:.0f}m | {audit_df.loc[x, 'origin_name']}")
    
    row = audit_df.loc[selected_idx]
    
    st.info(f"**Files in this group:** {row['file_count']}\n\n**Waste Loc:** {row['waste_location']}")
    
    # PDF Link (to the first file in the group)
    first_file = row['filename'][0]
    pdf_url = f"https://storage.googleapis.com/fta-form26r-library/full-set/{row['set_name']}/{first_file}#page={row['page_number']}"
    st.link_button("📄 View Sample PDF", pdf_url)
    
    st.write("---")
    st.subheader("Action")
    st.write(f"Apply to all {row['file_count']} files:")
    
    if st.button("✅ Verify All", use_container_width=True):
        new_entries = []
        for fname in row['filename']:
            new_entries.append({'filename': fname, 'audit_status': 'Verified', 'Master_Facility_ID': row['Master_Facility_ID']})
        
        res = pd.DataFrame(new_entries)
        path = 'data/processed/f26r_link_audit.parquet'
        pd.concat([pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame(), res]).to_parquet(path)
        st.cache_data.clear()
        st.rerun()
        
    if st.button("❌ Flag All as Mismatch", use_container_width=True):
        new_entries = []
        for fname in row['filename']:
            new_entries.append({'filename': fname, 'audit_status': 'Mismatch', 'Master_Facility_ID': row['Master_Facility_ID']})
        
        res = pd.DataFrame(new_entries)
        path = 'data/processed/f26r_link_audit.parquet'
        pd.concat([pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame(), res]).to_parquet(path)
        st.cache_data.clear()
        st.rerun()

with col2:
    st.subheader(f"Verification Map (Dist: {row['dist_m']:.1f}m)")
    
    # Map centered between points
    c_lat = (row['lat_final'] + row['Centroid_Lat']) / 2
    c_lon = (row['lon_final'] + row['Centroid_Lon']) / 2
    
    m = folium.Map(location=[c_lat, c_lon], zoom_start=17)
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='ESRI World Imagery', name='ESRI Satellite', overlay=False, control=True
    ).add_to(m)
    
    # F26R Point (Red)
    folium.Marker(
        location=[row['lat_final'], row['lon_final']],
        icon=folium.Icon(color='red', icon='trash'),
        popup=f"F26R ORIGIN\n{row['origin_addr']}"
    ).add_to(m)
    
    # MFL Centroid (Blue)
    folium.Marker(
        location=[row['Centroid_Lat'], row['Centroid_Lon']],
        icon=folium.Icon(color='blue', icon='home'),
        popup=f"MATCHED FACILITY\nID: {row['Master_Facility_ID']}\nTokens: {row['Name_Tokens']}"
    ).add_to(m)
    
    # Constituent Wells (Green Circles)
    facility_wells = wells[wells['Master_Facility_ID'] == row['Master_Facility_ID']]
    for _, w in facility_wells.iterrows():
        folium.CircleMarker(
            location=[w['LATITUDE_DECIMAL'], w['LONGITUDE_DECIMAL']],
            radius=4, color='green', fill=True, fill_color='green',
            popup=f"Well API: {w['API']}"
        ).add_to(m)
    
    st_folium(m, width=1000, height=600, key="audit_map")
    
    st.write(f"**Matched Facility Tokens:** {row['Name_Tokens']}")
    st.write(f"**Matched Facility County:** {row['County']}")
