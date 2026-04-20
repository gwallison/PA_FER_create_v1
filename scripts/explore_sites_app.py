import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import os
import numpy as np

st.set_page_config(layout="wide", page_title="FER Site Explorer")

@st.cache_data
def load_data():
    mfl_sites = pd.read_parquet('data/processed/master_facility_list_with_sites.parquet')
    golden = pd.read_parquet('data/processed/master_facility_list_deduped.parquet')
    return mfl_sites, golden

mfl, golden = load_data()

st.title("Facility Site Explorer")
st.markdown("Explore consolidated sites that group multiple operators and phases into a single physical location.")

# Sidebar Search
with st.sidebar:
    st.header("Search & Filter")
    
    # Filter for multi-operator sites by default as they are more interesting
    only_multi = st.checkbox("Show only multi-operator sites", value=True)
    
    if only_multi:
        search_df = golden[golden['Parent_Count'] > 1]
    else:
        search_df = golden

    st.write(f"Available Sites: {len(search_df)}")
    
    search_query = st.text_input("Search Site by Name Token", "").upper()
    if search_query:
        search_df = search_df[search_df['Name_Tokens'].apply(lambda x: any(search_query in t for t in x))]

    selected_site_id = st.selectbox("Select Site to Explore", 
                                    search_df.sort_values('Facility_Count', ascending=False)['Site_ID'].head(100))

if selected_site_id:
    site = golden[golden['Site_ID'] == selected_site_id].iloc[0]
    constituents = mfl[mfl['Site_ID'] == selected_site_id]
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Site Metadata")
        st.write(f"**Site ID:** `{selected_site_id}`")
        st.write(f"**Consolidated Names:** {', '.join(site['Name_Tokens'])}")
        st.write(f"**County/Municipality:** {site['County']}, {site['Municipality']}")
        st.write(f"**Entity Type:** {site['Entity_Type']}")
        st.write(f"**Constituent Facilities:** {site['Facility_Count']}")
        
        st.subheader("Operational History")
        st.info(f"**Operators (Parent IDs):** {', '.join(site['Parent_ID'])}")
        st.write(f"**Sources:** {', '.join(site['Source'])}")
        
        with st.expander("Constituent Well APIs"):
            st.write(site['Constituent_APIs'])

    with col2:
        st.subheader("Spatial Context")
        
        # Calculate bounds
        lat, lon = site['Centroid_Lat'], site['Centroid_Lon']
        
        if pd.notna(lat) and pd.notna(lon):
            m = folium.Map(location=[lat, lon], zoom_start=16, control_scale=True)
            
            # 1. Plot Site Constituents (GREEN)
            for _, row in constituents.iterrows():
                if pd.notna(row['Centroid_Lat']):
                    folium.Marker(
                        location=[row['Centroid_Lat'], row['Centroid_Lon']],
                        popup=f"Facility: {row['Master_Facility_ID']}<br>Operator: {row['Parent_ID']}<br>Names: {row['Name_Tokens']}",
                        icon=folium.Icon(color='green', icon='info-sign')
                    ).add_to(m)
                
            # 2. Find and Plot Neighbors (RED) - within ~1km
            neighbors = mfl[
                (mfl['Site_ID'] != selected_site_id) & 
                (np.abs(mfl['Centroid_Lat'] - lat) < 0.01) & 
                (np.abs(mfl['Centroid_Lon'] - lon) < 0.01)
            ]
            
            for _, row in neighbors.iterrows():
                folium.Marker(
                    location=[row['Centroid_Lat'], row['Centroid_Lon']],
                    popup=f"NEIGHBOR<br>ID: {row['Master_Facility_ID']}<br>Operator: {row['Parent_ID']}<br>Names: {row['Name_Tokens']}",
                    icon=folium.Icon(color='red', icon='warning-sign')
                ).add_to(m)

            st_folium(m, width=800, height=500)
            st.caption("Green markers represent facilities merged into this Site. Red markers are nearby distinct sites.")
        else:
            st.warning("No valid coordinates available for this site. Map cannot be displayed.")

    st.subheader("Facility Breakdown")
    st.dataframe(constituents[['Master_Facility_ID', 'Parent_ID', 'Name_Tokens', 'Source', 'Entity_Type']])

else:
    st.info("Select a Site ID from the sidebar to begin exploration.")
