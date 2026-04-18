import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import numpy as np

# Page config
st.set_page_config(layout="wide", page_title="PA WellPad Verification Tool")

@st.cache_data
def load_data():
    # Only load required columns for MFL to speed up selection
    mfl_cols = ['Master_Facility_ID', 'Centroid_Lat', 'Centroid_Lon', 'County', 'Name_Tokens', 'Entity_Type']
    mfl = pd.read_parquet('data/processed/master_facility_list_final.parquet', columns=mfl_cols)
    mfl = mfl.dropna(subset=['Centroid_Lat', 'Centroid_Lon'])
    
    wells_cols = ['Master_Facility_ID', 'API', 'FARM', 'OPERATOR', 'WELL_TYPE', 'WELL_STATUS', 'LATITUDE_DECIMAL', 'LONGITUDE_DECIMAL', 'SPUD_DATE']
    wells = pd.read_parquet('data/interim/wells_clustered.parquet', columns=wells_cols)
    wells = wells.dropna(subset=['LATITUDE_DECIMAL', 'LONGITUDE_DECIMAL'])
    
    # Convert SPUD_DATE to datetime (it's stored as strings)
    wells['SPUD_DATE_DT'] = pd.to_datetime(wells['SPUD_DATE'], errors='coerce')
    
    # Pre-calculate operator, well count, statuses, and spud range for MFL
    well_stats = wells.groupby('Master_Facility_ID').agg(
        OPERATOR=('OPERATOR', 'first'),
        well_count=('API', 'count'),
        statuses=('WELL_STATUS', lambda x: list(x.unique())),
        min_spud=('SPUD_DATE_DT', 'min'),
        max_spud=('SPUD_DATE_DT', 'max')
    ).reset_index()
    mfl = mfl.merge(well_stats, on='Master_Facility_ID', how='left')
    mfl['well_count'] = mfl['well_count'].fillna(0).astype(int)
    mfl['statuses'] = mfl['statuses'].fillna("").apply(lambda x: x if isinstance(x, list) else [])
    
    # Pre-calculate display name
    mfl['display_name'] = mfl.apply(lambda r: f"{r['Master_Facility_ID'][:8]}... ({r['well_count']} wells) | {r['Name_Tokens']}", axis=1)
    
    return mfl, wells

mfl, wells = load_data()

# Session State for Selection
if 'selected_pad_id' not in st.session_state:
    st.session_state.selected_pad_id = None

st.sidebar.title("Filters")

# Filters
counties = sorted(mfl['County'].dropna().unique())
selected_county = st.sidebar.selectbox("Select County", ["All"] + counties)

filtered_mfl = mfl
if selected_county != "All":
    filtered_mfl = filtered_mfl[filtered_mfl['County'] == selected_county]

operators = sorted(filtered_mfl['OPERATOR'].dropna().unique())
selected_operator = st.sidebar.selectbox("Select Operator", ["All"] + operators)

if selected_operator != "All":
    filtered_mfl = filtered_mfl[filtered_mfl['OPERATOR'] == selected_operator]

# Well Status Filter
all_statuses = sorted(list(set([s for sublist in mfl['statuses'] for s in sublist])))
selected_statuses = st.sidebar.multiselect("Select Well Statuses", all_statuses)

if selected_statuses:
    # Filter for pads that have AT LEAST ONE well matching the selected statuses
    filtered_mfl = filtered_mfl[filtered_mfl['statuses'].apply(lambda x: any(s in selected_statuses for s in x))]

# Spud Date Filter
valid_spuds = mfl['min_spud'].dropna()
if not valid_spuds.empty:
    min_date = valid_spuds.min().to_pydatetime()
    max_date = valid_spuds.max().to_pydatetime()
    
    # Ensure min < max
    if min_date < max_date:
        selected_dates = st.sidebar.slider(
            "Select Spud Date Range",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="YYYY-MM-DD"
        )
        
        # Filter for pads whose spud range overlaps with the selected range
        filtered_mfl = filtered_mfl[
            (filtered_mfl['max_spud'] >= pd.Timestamp(selected_dates[0])) &
            (filtered_mfl['min_spud'] <= pd.Timestamp(selected_dates[1]))
        ]

search_term = st.sidebar.text_input("Search by Token or Pad ID")
if search_term:
    filtered_mfl = filtered_mfl[
        filtered_mfl['Master_Facility_ID'].str.contains(search_term, case=False) |
        filtered_mfl['Name_Tokens'].apply(lambda x: search_term.upper() in [t.upper() for t in x] if x is not None else False)
    ]

# Radius for details
radius_km = st.sidebar.slider("Radius for nearby wells (km)", 0.1, 5.0, 1.0)
radius_deg = radius_km / 111.0

# Tabs
tab_browser, tab_details = st.tabs(["Pad Browser Map", "Pad Detailed View"])

with tab_browser:
    st.subheader("Click a pad to select it")
    
    # Limit markers for browser performance
    browser_limit = 500
    browser_df = filtered_mfl.head(browser_limit)
    
    if len(filtered_mfl) > browser_limit:
        st.info(f"Showing first {browser_limit} of {len(filtered_mfl)} pads. Filter to see more.")
    
    if not browser_df.empty:
        # Center on the first item or the selected item
        center_lat = browser_df['Centroid_Lat'].mean()
        center_lon = browser_df['Centroid_Lon'].mean()
        
        m_browser = folium.Map(location=[center_lat, center_lon], zoom_start=10)
        
        for _, r in browser_df.iterrows():
            color = 'blue'
            if r['Master_Facility_ID'] == st.session_state.selected_pad_id:
                color = 'green'
            
            # Scale radius: base 4 + square root of well count
            marker_radius = 4 + (np.sqrt(r['well_count']) * 2)
                
            folium.CircleMarker(
                location=[r['Centroid_Lat'], r['Centroid_Lon']],
                radius=marker_radius,
                color=color,
                fill=True,
                popup=f"ID: {r['Master_Facility_ID']}<br>Wells: {r['well_count']}<br>Tokens: {r['Name_Tokens']}",
                tooltip=r['Master_Facility_ID'] # Used for selection
            ).add_to(m_browser)
            
        map_data = st_folium(m_browser, width=1200, height=600, key="browser_map")
        
        # Handle Map Click
        if map_data and map_data.get("last_object_clicked_tooltip"):
            new_id = map_data["last_object_clicked_tooltip"]
            if new_id != st.session_state.selected_pad_id:
                st.session_state.selected_pad_id = new_id
                st.rerun()

# Sidebar Dropdown sync with Session State
pad_options = filtered_mfl.head(200) # Increased limit for search
if not pad_options.empty:
    # If the current selected_pad_id is not in the filtered options, prepend it so it's visible
    if st.session_state.selected_pad_id and st.session_state.selected_pad_id not in pad_options['Master_Facility_ID'].values:
        extra_pad = mfl[mfl['Master_Facility_ID'] == st.session_state.selected_pad_id]
        if not extra_pad.empty:
            pad_options = pd.concat([extra_pad, pad_options])

    selected_pad_id = st.sidebar.selectbox(
        "Direct Selection", 
        options=pad_options['Master_Facility_ID'],
        index=list(pad_options['Master_Facility_ID']).index(st.session_state.selected_pad_id) if st.session_state.selected_pad_id in pad_options['Master_Facility_ID'].values else 0,
        format_func=lambda x: pad_options[pad_options['Master_Facility_ID'] == x]['display_name'].values[0]
    )
    
    if selected_pad_id != st.session_state.selected_pad_id:
        st.session_state.selected_pad_id = selected_pad_id
        st.rerun()

with tab_details:
    if st.session_state.selected_pad_id:
        pad_row = mfl[mfl['Master_Facility_ID'] == st.session_state.selected_pad_id].iloc[0]
        st.title(f"Pad Verification: {pad_row['Master_Facility_ID']}")
        
        assigned_wells = wells[wells['Master_Facility_ID'] == st.session_state.selected_pad_id]
        lat, lon = pad_row['Centroid_Lat'], pad_row['Centroid_Lon']
        
        nearby_wells = wells[
            (wells['LATITUDE_DECIMAL'] > lat - radius_deg) & 
            (wells['LATITUDE_DECIMAL'] < lat + radius_deg) & 
            (wells['LONGITUDE_DECIMAL'] > lon - radius_deg) & 
            (wells['LONGITUDE_DECIMAL'] < lon + radius_deg) &
            (wells['Master_Facility_ID'] != st.session_state.selected_pad_id)
        ]
        
        col1, col2 = st.columns([3, 1])
        with col1:
            m_detail = folium.Map(location=[lat, lon], zoom_start=16)
            folium.TileLayer(
                tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
                attr='ESRI World Imagery', name='ESRI Satellite', overlay=False, control=True
            ).add_to(m_detail)
            
            for _, w in assigned_wells.iterrows():
                folium.CircleMarker(
                    location=[w['LATITUDE_DECIMAL'], w['LONGITUDE_DECIMAL']],
                    radius=6, color='green', fill=True, fill_color='green',
                    popup=f"API: {w['API']}<br>Farm: {w['FARM']}"
                ).add_to(m_detail)
                
            for _, w in nearby_wells.iterrows():
                # Visual logic for nearby
                is_active = w['WELL_STATUS'] == 'Active'
                is_modern = w['SPUD_DATE_DT'].year >= 2000 if pd.notnull(w['SPUD_DATE_DT']) else False
                
                # Color: Red for Active, Orange for Not Active
                border_color = 'red' if is_active else 'orange'
                # Fill: Solid for Modern, Hollow for Historic
                fill_opacity = 1.0 if is_modern else 0
                
                folium.CircleMarker(
                    location=[w['LATITUDE_DECIMAL'], w['LONGITUDE_DECIMAL']],
                    radius=5, 
                    color=border_color, 
                    fill=True, 
                    fill_color=border_color,
                    fill_opacity=fill_opacity,
                    popup=f"API: {w['API']}<br>Operator: {w['OPERATOR']}<br>Status: {w['WELL_STATUS']}<br>Spud: {w['SPUD_DATE']}<br>Pad: {w['Master_Facility_ID']}"
                ).add_to(m_detail)
                
            st_folium(m_detail, width=900, height=600, key="detail_map")
            
        with col2:
            st.subheader("Pad Details")
            st.write(f"**Entity:** {pad_row['Entity_Type']}")
            st.write(f"**Wells:** {len(assigned_wells)}")
            st.write(f"**County:** {pad_row['County']}")
            st.write(f"**Tokens:** {pad_row['Name_Tokens']}")
            
            st.write("---")
            st.subheader("Map Legend")
            st.markdown("🟢 **Assigned Well**")
            st.markdown("🔴 **Nearby Active** (Red border)")
            st.markdown("🟠 **Nearby Inactive** (Orange border)")
            st.markdown("⚫ **Solid:** Spud ≥ 2000")
            st.markdown("⚪ **Hollow:** Spud < 2000 or NaN")
            
        st.subheader("Assigned Wells")
        st.dataframe(assigned_wells[['API', 'FARM', 'OPERATOR', 'WELL_TYPE', 'WELL_STATUS']])
        
        if not nearby_wells.empty:
            st.subheader("Nearby Other Wells")
            st.dataframe(nearby_wells[['API', 'FARM', 'OPERATOR', 'WELL_STATUS', 'Master_Facility_ID']])
    else:
        st.info("Select a pad from the Browser Map or the Sidebar to see details.")
