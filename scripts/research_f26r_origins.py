import pandas as pd
import re

def parse_waste_location(text):
    if not isinstance(text, str):
        return None, None
    
    # Split by common separators like " - " or " @ "
    parts = re.split(r'\s*-\s*|\s*@\s*', text, maxsplit=1)
    
    name = parts[0].strip() if len(parts) > 0 else None
    maybe_addr = parts[1].strip() if len(parts) > 1 else None
    
    # Validate address
    # Addresses usually start with numbers and are longer than 5 chars
    # Avoid things like "1H Pad" or "5H"
    if maybe_addr:
        if re.match(r'^\d{2,}\s+', maybe_addr) or re.search(r'\b(Road|Rd|Lane|Ln|Drive|Dr|Boulevard|Blvd|Street|St|Ave|Pike|Hwy|Rt|Route|Way)\b', maybe_addr, re.I):
            addr = maybe_addr
        else:
            # It's likely part of the name (e.g., "Pad A - 1H")
            name = f"{name} - {maybe_addr}"
            addr = None
    else:
        addr = None
            
    # If no separator, the whole thing might be an address
    if addr is None and name:
        if re.match(r'^\d{3,}\s+\w+', name): # e.g. "479 Taylortown"
            addr = name
            name = None
            
    return name, addr

def analyze_origins():
    print("Loading raw F26R data...")
    df = pd.read_parquet('data/raw/all_harvested_form26r_v2.parquet')
    
    # Group by filename to get unique origin sessions
    origins = df.groupby('filename').agg({
        'waste_location': 'first',
        'company_name': 'first'
    }).reset_index()
    
    print(f"Unique files (origins): {len(origins)}")
    
    print("\nParsing waste_location...")
    parsed = origins['waste_location'].apply(parse_waste_location)
    origins['parsed_name'] = [x[0] for x in parsed]
    origins['parsed_addr'] = [x[1] for x in parsed]
    
    print("\nSample of parsed origins:")
    print(origins[['waste_location', 'parsed_name', 'parsed_addr']].head(20))
    
    # Count how many have addresses
    has_addr = origins['parsed_addr'].notna().sum()
    print(f"\nOrigins with extracted addresses: {has_addr} ({has_addr/len(origins)*100:.1f}%)")
    
    return origins

if __name__ == "__main__":
    origins = analyze_origins()
    origins.to_csv('data/interim/f26r_origin_analysis.csv', index=False)
