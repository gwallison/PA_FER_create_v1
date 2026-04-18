# FER Exploration Journal: From Companies to Facilities

## [2026-04-17] - Transition to Phase 2: Facility Linkage
**Objective:** To identify and associate related oil and gas facilities in various PA data sets.

### Phase 2: Building the Master Facility List (MFL)

#### 1. Spatial Tiering & Pad Clustering
- **The Scale Problem:** DEP data is well-level; F26R data is pad-level. 
- **Solution:** Clustered 223,768 DEP wells into **106,611 "Virtual Pads"** using Agglomerative Clustering (400m threshold) constrained by CER `Parent_ID`.
- **Result:** Created the foundation for the Master Facility List (MFL).

#### 2. Canonical F26R Origin Resolution (Updated 2026-04-18)
- **Strategy Shift:** Moved from individual record resolution to "Filename-level Canonical Origins." Each F26R file represents a single source event; deduplicating at this level increased precision and efficiency.
- **Improved Parsing:** Enhanced `waste_location` parsing to handle complex patterns:
    - Coordinate extraction (Lat/Lon) directly from text.
    - Street address extraction using keyword-aware regex (Road, Rd, Turnpike, etc.).
    - Parentheses handling (e.g., "(Pad Name) Address").
    - Noise cleaning (removing emails, phone numbers, and form boilerplate).
- **Evidence Fusion:** Implemented a multi-pass resolution logic that prioritizes:
    1. Direct extracted coordinates + Parent ID.
    2. Geocoded addresses + Parent ID.
    3. Spatial proximity only (for acquisitions).
    4. Fuzzy name token matching + Parent ID.
- **Linkage Achievement:** **12,685 out of 13,464 unique origin files linked (94.2%)**.

### Key Metrics
| Metric | Count |
| :--- | :--- |
| **Total Master Facilities** | **113,479** |
| - From DEP Inventory | 106,611 |
| - Synthesized from F26R | 6,868 |
| **F26R Linkage Rate (Origin Files)** | **94.2% (12,685 / 13,464)** |
| - Pass 1 (Parent + Spatial) | 1,399 |
| - Pass 2 (Spatial Only) | 557 |
| - Pass 3 (Parent + Name) | 10,729 |

### Next Steps
- **Unlinked Analysis:** Triage the remaining 779 unlinked files.
- **Streamlit Verification:** Use the newly created `verify_pads_app.py` to visually confirm high-confidence vs. relaxed spatial matches.
- **Master List Enrichment:** Finalize county/municipality metadata for synthesized facilities.

---

## 2026-04-18: WellPad Clustering Considerations
- **Insight:** Clustering is primarily intended to treat similar wells that are geographically close as a single functional group (the "Pad"). This grouping is most meaningful for active, recently drilled, or recently active wells.
- **Challenge:** We must determine how to handle inactive or historic wells (Spud < 2000) that are near these groups. Should they be absorbed into the modern pad entity, or do they represent a distinct legacy infrastructure layer?
- **Idea:** It might be useful to implement a secondary `Site_ID` based on pure spatial clustering, regardless of company ownership. This would provide a long-term "historical record" for a particular location across different operators and eras.
- **Goal:** Ensure the Master Facility List distinguishes between modern operational clusters and legacy sites to keep downstream resolution accurate.
