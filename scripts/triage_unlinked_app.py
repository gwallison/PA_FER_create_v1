import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import os
import re
import numpy as np
from geopy.geocoders import Nominatim, GoogleV3
from geopy.extra.rate_limiter import RateLimiter

st.set_page_config(layout="wide", page_title="F26R Unlinked Triage")

# Initialize Session State for geocoded results
if 'geocoded_coords' not in st.session_state:
    st.session_state.geocoded_coords = None

def get_geocoder():
    # Priority: Google Maps API key from environment
    google_key = os.environ.get('Google-maps-geocoding-api')
    if google_key:
        return GoogleV3(api_key=google_key)
    else:
        # Fallback to ArcGIS if Google key is missing (better than Nominatim)
        from geopy.geocoders import ArcGIS
        return ArcGIS()

def parse_coords(coord_str):
    if not coord_str: return None, None
    try:
        parts = re.split(r'[,/ ]+', coord_str.strip())
        if len(parts) >= 2:
            lat = float(parts[0])
            lon = float(parts[1])
            return lat, lon
    except:
        pass
    return None, None

@st.cache_data
def load_triage_data():
    mfl = pd.read_parquet('data/processed/master_facility_list_final.parquet')
    origins = pd.read_parquet('data/interim/f26r_origins.parquet')
    links = pd.read_parquet('data/processed/f26r_origin_links.parquet')
    
    # 1. Remove files already linked automatically
    unlinked_files = origins[~origins['filename'].isin(links['filename'])].copy()
    
    # 2. Remove files already linked manually
    manual_path = 'data/processed/f26r_manual_links.parquet'
    if os.path.exists(manual_path):
        manual_links = pd.read_parquet(manual_path)
        unlinked_files = unlinked_files[~unlinked_files['filename'].isin(manual_links['filename'])]

    # 3. Remove files already marked for synthesis
    synth_path = 'data/processed/f26r_synthesis_markers.parquet'
    if os.path.exists(synth_path):
        synth_markers = pd.read_parquet(synth_path)
        unlinked_files = unlinked_files[~unlinked_files['filename'].isin(synth_markers['filename'])]
    
    if unlinked_files.empty:
        return mfl, pd.DataFrame()

    # 4. Group by Metadata to collapse duplicates for easier triage
    group_cols = ['waste_location', 'company_name', 'origin_name', 'origin_addr', 'origin_lat', 'origin_lon', 'set_name', 'page_number']
    unlinked_grouped = unlinked_files.groupby(group_cols, dropna=False).agg({
        'filename': lambda x: list(x)
    }).reset_index()
    
    unlinked_grouped['file_count'] = unlinked_grouped['filename'].apply(len)
    
    return mfl, unlinked_grouped

mfl, unlinked = load_triage_data()

st.sidebar.title("App Controls")
if st.sidebar.button("Clear Data Cache"):
    st.cache_data.clear()
    st.rerun()

st.title("F26R Unlinked Origin Triage")
st.write(f"Remaining Unlinked Cases: {len(unlinked)} (Files: {unlinked['file_count'].sum() if not unlinked.empty else 0})")

if unlinked.empty:
    st.success("All origins resolved!")
    st.stop()

# --- Selection Section ---
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Unlinked List")
    u_search = st.text_input("Filter Unlinked List", "")
    display_u = unlinked
    if u_search:
        display_u = unlinked[unlinked['waste_location'].str.contains(u_search, case=False, na=False)]
    
    if display_u.empty:
        st.warning("No unlinked origins match your filter.")
        selected_idx = None
    else:
        selected_idx = st.selectbox("Select Case to Resolve", display_u.index, 
                                    format_func=lambda x: f"[{unlinked.loc[x, 'file_count']} files] {unlinked.loc[x, 'waste_location']}")
    
    if selected_idx is not None:
        item = unlinked.loc[selected_idx]
        st.info(f"**Files in this case:** {item['file_count']}\n\n**Company:** {item['company_name']}")
        
        # PDF Link (Sample)
        pdf_url = f"https://storage.googleapis.com/fta-form26r-library/full-set/{item['set_name']}/{item['filename'][0]}#page={item['page_number']}"
        st.link_button("📄 View Sample PDF", pdf_url)
        
        if item['origin_addr']:
            st.write(f"**Extracted Addr:** {item['origin_addr']}")
            if st.button("🌍 Geocode Extracted Address"):
                geolocator = get_geocoder()
                geocode = RateLimiter(geolocator.geocode, min_delay_seconds=0.5) # Google is faster than OSM
                
                query = item['origin_addr']
                if 'PA' not in query.upper() and 'PENNSYLVANIA' not in query.upper():
                    query += ", Pennsylvania"
                
                try:
                    location = geocode(query)
                    if location:
                        st.session_state.geocoded_coords = f"{location.latitude}, {location.longitude}"
                        st.success(f"Found via Google: {st.session_state.geocoded_coords}")
                        st.rerun()
                    else:
                        st.error("Address not found by geocoder.")
                except Exception as e:
                    st.error(f"Geocoding error: {e}")

        if pd.notna(item['origin_lat']):
            st.write(f"**Extracted Coords:** {item['origin_lat']}, {item['origin_lon']}")

with col2:
    if selected_idx is not None:
        st.subheader("Search Master Facility List")
        search_mode = st.radio("Search Mode", ["Name/County", "Coordinates", "Direct ID"], horizontal=True)
        results = mfl
        
        # Determine base coordinates for the case
        base_lat, base_lon = item['origin_lat'], item['origin_lon']
        if st.session_state.geocoded_coords:
            g_lat, g_lon = parse_coords(st.session_state.geocoded_coords)
            if g_lat:
                base_lat, base_lon = g_lat, g_lon

        sort_lat, sort_lon = base_lat, base_lon

        if search_mode == "Name/County":
            search_col1, search_col2 = st.columns(2)
            with search_col1:
                name_q = st.text_input("Search Name (Tokens)", value=item['origin_name'] if item['origin_name'] else "")
            with search_col2:
                county_q = st.selectbox("Filter by County", ["All"] + sorted(mfl['County'].dropna().unique().tolist()))
            if name_q:
                results = results[results['Name_Tokens'].apply(lambda x: any(name_q.upper() in t for t in x))]
            if county_q != "All":
                results = results[results['County'] == county_q]
                
        elif search_mode == "Coordinates":
            # Use geocoded coords if available as default
            coord_val = st.session_state.geocoded_coords if st.session_state.geocoded_coords else (f"{item['origin_lat']}, {item['origin_lon']}" if pd.notna(item['origin_lat']) else "")
            coord_input = st.text_input("Paste Coordinates (Lat, Lon)", value=coord_val)
            search_lat, search_lon = parse_coords(coord_input)
            if search_lat is not None:
                sort_lat, sort_lon = search_lat, search_lon
            elif coord_input:
                st.warning("Please enter valid coordinates: 'Lat, Lon'")
            
        elif search_mode == "Direct ID":
            id_q = st.text_input("Enter Master_Facility_ID")
            if id_q:
                results = results[results['Master_Facility_ID'] == id_q]

        st.write(f"Found {len(results)} candidates")
        
        if not results.empty:
            if sort_lat is not None:
                results = results.copy()
                results['dist_deg'] = ((results['Centroid_Lat'] - sort_lat)**2 + 
                                    (results['Centroid_Lon'] - sort_lon)**2)**0.5
                results = results.sort_values('dist_deg')

            st.dataframe(results[['Master_Facility_ID', 'Name_Tokens', 'County', 'Entity_Type', 'Parent_ID']].head(20))
            link_id = st.selectbox("Select Match to Verify on Map", results['Master_Facility_ID'].head(50))
            
            if link_id:
                cand_row = mfl[mfl['Master_Facility_ID'] == link_id].iloc[0]
                st.subheader("Verification Map")
                
                # Verify candidate coords
                if pd.notna(cand_row['Centroid_Lat']) and pd.notna(cand_row['Centroid_Lon']):
                    m = folium.Map(location=[cand_row['Centroid_Lat'], cand_row['Centroid_Lon']], zoom_start=16)
                    folium.TileLayer(tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
                        attr='ESRI World Imagery', name='ESRI Satellite', overlay=False, control=True).add_to(m)
                    
                    folium.Marker(location=[cand_row['Centroid_Lat'], cand_row['Centroid_Lon']], icon=folium.Icon(color='blue'), popup="CANDIDATE").add_to(m)
                    
                    # Verify unlinked coords
                    if sort_lat is not None and sort_lon is not None and pd.notna(sort_lat) and pd.notna(sort_lon):
                        folium.Marker(location=[sort_lat, sort_lon], icon=folium.Icon(color='red'), popup="UNLINKED").add_to(m)
                    
                    st_folium(m, width=900, height=400, key="verify_map")
                else:
                    st.warning("Candidate facility lacks valid coordinates for mapping.")
            
            if st.button(f"Confirm Manual Link for all {item['file_count']} files"):
                new_entries = [{'filename': fname, 'Master_Facility_ID': link_id, 'Confidence': 1.0, 'Match_Method': 'Manual Triage'} for fname in item['filename']]
                manual_path = 'data/processed/f26r_manual_links.parquet'
                pd.concat([pd.read_parquet(manual_path) if os.path.exists(manual_path) else pd.DataFrame(), pd.DataFrame(new_entries)]).drop_duplicates('filename').to_parquet(manual_path)
                st.success(f"Linked {item['file_count']} files to {link_id}")
                st.session_state.geocoded_coords = None # Reset for next case
                st.cache_data.clear()
                st.rerun()

        st.write("---")
        st.subheader("Synthesis (New Facility)")
        # Use geocoded coords if available as default
        synth_coord_val = st.session_state.geocoded_coords if st.session_state.geocoded_coords else (f"{sort_lat}, {sort_lon}" if sort_lat else "")
        synth_coord_input = st.text_input("Final Coordinates (Paste Lat, Lon)", value=synth_coord_val)
        synth_lat, synth_lon = parse_coords(synth_coord_input)

        if st.button(f"Mark all {item['file_count']} files for Synthesis"):
            if synth_lat is None:
                st.error("Invalid coordinates. Please paste 'Lat, Lon'.")
            else:
                new_markers = []
                for fname in item['filename']:
                    new_markers.append({
                        'filename': fname, 'waste_location': item['waste_location'], 'company_name': item['company_name'],
                        'origin_name': item['origin_name'], 'origin_addr': item['origin_addr'],
                        'origin_lat': synth_lat, 'origin_lon': synth_lon
                    })
                synth_path = 'data/processed/f26r_synthesis_markers.parquet'
                pd.concat([pd.read_parquet(synth_path) if os.path.exists(synth_path) else pd.DataFrame(), pd.DataFrame(new_markers)]).drop_duplicates('filename').to_parquet(synth_path)
                st.warning(f"Marked {item['file_count']} files for synthesis.")
                st.session_state.geocoded_coords = None # Reset for next case
                st.cache_data.clear()
                st.rerun()
    else:
        st.info("Select an origin from the list on the left to begin.")
