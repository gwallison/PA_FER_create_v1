import pandas as pd
import re
import os

def clean_noise(text):
    if not isinstance(text, str): return ""
    # Remove emails
    text = re.sub(r'\S+@\S+', ' ', text)
    # Remove phone numbers
    text = re.sub(r'\d{3}[-.\s]??\d{3}[-.\s]??\d{4}', ' ', text)
    # Remove common form noise like "Suffix Yes/No"
    text = re.sub(r'\bSuffix\b.*?\b(Yes|No)\b', ' ', text, flags=re.I)
    # Remove boilerplate questions
    text = re.sub(r'Is the waste generated at the Company Mailing Address.*', ' ', text, flags=re.I)
    # Remove other names often found in noise
    text = re.sub(r'\b(Smelko|John)\b', ' ', text, flags=re.I)
    return " ".join(text.split())

def parse_waste_location(text):
    if not isinstance(text, str):
        return None, None, None, None
    
    # First clean the overall noise
    text = clean_noise(text)
    
    # 0. Extract Coordinates if present
    # Matches patterns like 41.9059, -75.9217 or N 41.905° W 75.92°
    lat, lon = None, None
    coord_match = re.search(r'([NS]?\s*[34]\d\.\d{3,})°?\s*[,/ ]\s*([EW]?\s*-?[78]\d\.\d{3,})°?', text, re.I)
    if coord_match:
        lat_str = re.sub(r'[NS\s°]', '', coord_match.group(1), flags=re.I)
        lon_str = re.sub(r'[EW\s°]', '', coord_match.group(2), flags=re.I)
        try:
            lat = float(lat_str)
            lon = float(lon_str)
            # Longitude in PA is always negative
            if lon > 0: lon = -lon
        except:
            pass

    # Strip leading/trailing separators or whitespace
    text = re.sub(r'^[-\s@,:/]+', '', text).strip()
    if not text: return None, None, lat, lon

    # 1. Handle (Pad Name) Address pattern
    paren_match = re.match(r'^\((.*?)\)\s*(.*)', text)
    if paren_match:
        name = paren_match.group(1).strip()
        maybe_addr = paren_match.group(2).strip()
        if re.match(r'^\d{1,5}\s+', maybe_addr) or re.search(r'\b(Road|Rd|Lane|Ln|Drive|Dr|Boulevard|Blvd|Street|St|Ave|Pike|Hwy|Rt|Route|Way|Trail|Hollow|Turnpike)\b', maybe_addr, re.I):
            return name, maybe_addr, lat, lon

    # 2. Split by common separators ( - , @ , : , / , or comma if followed by number)
    parts = re.split(r'\s*[-@:/]\s*|,\s*(?=\d)', text)
    
    if len(parts) > 1:
        last_part = parts[-1].strip()
        # If the last part is just the coordinates we already extracted, look at the previous part
        if coord_match and coord_match.group(0) in last_part:
            if len(parts) > 2:
                last_part = parts[-2].strip()
                parts = parts[:-1]
            else:
                # No address, just Name / Coords
                return parts[0].strip(), None, lat, lon

        is_addr = re.search(r'\b(Road|Rd|Lane|Ln|Drive|Dr|Boulevard|Blvd|Street|St|Ave|Pike|Hwy|Rt|Route|Way|Trail|Hollow|Turnpike)\b', last_part, re.I)
        if not is_addr and re.match(r'^\d{2,5}\s+[A-Z]', last_part, re.I):
            if not re.search(r'\b(\d+[HV]|PAD|UNIT|WELL|FT|FT\.)\b', last_part, re.I):
                is_addr = True
        
        if is_addr:
            name = " - ".join([p.strip() for p in parts[:-1]])
            return name, last_part, lat, lon

    # 3. Handle Name followed by Address without separator
    addr_match = re.search(r'(\d{1,5}\s+[\w\s]{3,}\b(Road|Rd|Lane|Ln|Drive|Dr|Boulevard|Blvd|Street|St|Ave|Pike|Hwy|Rt|Route|Way|Pike|Hwy|Trail|Hollow|Turnpike)\b.*)', text, re.I)
    if addr_match:
        addr = addr_match.group(1).strip()
        name = text[:addr_match.start()].strip()
        name = re.sub(r'[-\s@,:/]+$', '', name)
        return name, addr, lat, lon

    # Fallback
    if re.search(r'\b(Road|Rd|Lane|Ln|Drive|Dr|Boulevard|Blvd|Street|St|Ave|Pike|Hwy|Rt|Route|Way|Trail|Hollow|Turnpike)\b', text, re.I):
        addr_split = re.split(r'\s+(\d{3,5}\s+(?!PAD|UNIT|WELL|.*[HV]\b)\w+.*)', text, maxsplit=1, flags=re.I)
        if len(addr_split) > 1:
            return addr_split[0].strip(), addr_split[1].strip(), lat, lon
            
    return text, None, lat, lon

def prepare_origins():
    print("Loading raw F26R data...")
    df = pd.read_parquet('data/raw/all_harvested_form26r_v2.parquet')
    
    print("Grouping by filename to isolate origins...")
    origins = df.groupby('filename').agg({
        'waste_location': 'first',
        'company_name': 'first',
        'date_prepared': 'first'
    }).reset_index()
    
    print("Parsing waste_location...")
    parsed = origins['waste_location'].apply(parse_waste_location)
    origins['origin_name'] = [x[0] for x in parsed]
    origins['origin_addr'] = [x[1] for x in parsed]
    origins['origin_lat'] = [x[2] for x in parsed]
    origins['origin_lon'] = [x[3] for x in parsed]
    
    # Save prepared origins
    os.makedirs('data/interim', exist_ok=True)
    origins.to_parquet('data/interim/f26r_origins.parquet')
    print(f" - Saved {len(origins)} unique origins to data/interim/f26r_origins.parquet")

if __name__ == "__main__":
    prepare_origins()
