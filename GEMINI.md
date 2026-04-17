# Project: PA Facility Entity Resolution (FER)

## Objective
To develop a probabilistic Record Linkage system that associates disparate oil and gas infrastructure records (wells, pads, compressors) from datasets like `PA_wells` and `F26R` into a single Master Facility List.

## Prerequisites
- **CER Integration:** All incoming records must first be processed through the Company Entity Resource to assign a `Parent_ID`. This is a hard constraint for blocking spatial joins.

## Core Strategy: Hierarchical Multi-Pass Resolution
1. **Virtual Pad Clustering:** Individual wellheads from `PA_wells` (DEP) are clustered into "Master Facilities" using Agglomerative Clustering (400m threshold) blocked by `Parent_ID`.
2. **Strict Resolution (Pass 1):** Link `F26R` origin locations to the Master Facility List (MFL) using strict `Parent_ID` blocking + Name Token intersection + Spatial proximity.
3. **Relaxed Spatial Join (Pass 2):** Recover links where companies differ but locations are identical (within 500m), targeting acquisitions or contractor reporting.
4. **Entity Synthesis (Pass 3):** For persistent non-matches, synthesize new "Master Facility" records from `F26R` origin metadata to ensure 100% data coverage.

## Master Facility Schema (The "Golden Record")
- `Master_Facility_ID`: A unique UUID (or 'SYN-' prefixed ID for synthesized records).
- `Entity_Type`: [Well Pad, Compressor Station, Impoundment].
- `Parent_ID`: Canonical ID from the CER.
- `Centroid_Lat/Lon`: The centroid of all constituent wells or the best available geocoded origin.
- `Name_Tokens`: Aggregated set of cleaned keyword tokens for fuzzy matching.
- `Source`: [DEP Inventory, F26R Synthesis].

## Technical Stack
- **Core:** Python (Pandas, Scikit-Learn, Scipy, Geopy).
- **Spatial:** KDTree for fast O(log n) proximity searches.
- **Linkage:** Probabilistic scoring based on Name overlap and Spatial distance.

## Resolution Performance
- **Phase 1 Complete:** 113,479 unique facilities defined.
- **F26R Coverage:** 100% of records associated with a Master ID (33.4% matched to DEP, 66.6% synthesized).
