# FER Exploration Journal: From Companies to Facilities

## [2026-04-17] - Transition to Phase 2: Facility Linkage
**Objective:** To identify and associate related oil and gas facilities in various PA data sets.

### Phase 2: Building the Master Facility List (MFL)

#### 1. Spatial Tiering & Pad Clustering
- **The Scale Problem:** DEP data is well-level; F26R data is pad-level. 
- **Solution:** Clustered 223,768 DEP wells into **106,611 "Virtual Pads"** using Agglomerative Clustering (400m threshold) constrained by CER `Parent_ID`.
- **Result:** Created the foundation for the Master Facility List (MFL).

#### 2. F26R Coordinate Recovery
- **Tier 1 Extraction:** Identified 858 records with high-precision coordinates in raw text fields.
- **Tier 2 Geocoding:** Initialized a rate-limited geocoding pipeline via Nominatim, processing 388 unique addresses to recover spatial locations for several thousand waste records.

#### 3. Multi-Pass Facility Resolution
- **Pass 1 (Strict):** Linked 10,121 records using exact `Parent_ID` and name token overlap.
- **Pass 2 (Relaxed Spatial):** Linked an additional 1,899 records by ignoring operator mismatches within 500m of known facilities (capturing acquisitions/contractors).
- **Pass 3 (Synthesis):** For the remaining 23,936 records, synthesized **6,868 new Master Facilities** from F26R metadata to ensure 100% data coverage.

### Key Metrics
| Metric | Count |
| :--- | :--- |
| **Total Master Facilities** | **113,479** |
| - From DEP Inventory | 106,611 |
| - Synthesized from F26R | 6,868 |
| **F26R Linkage Rate** | **100% (All 35,956 records)** |
| - Linked to DEP Pads | 12,020 (33.4%) |
| - Linked to Synth Pads | 23,936 (66.6%) |

### Next Steps
- **Validation:** Visual inspection of cluster centroids and name token accuracy.
- **Geocoding Expansion:** Continue processing the remaining ~3,000 unique F26R addresses.
- **Master List Enrichment:** Join additional metadata (County, Municipality) to synthesized facilities.        

---

## 2026-04-18: WellPad Clustering Considerations
- **Insight:** Clustering is primarily intended to treat similar wells that are geographically close as a single functional group (the "Pad"). This grouping is most meaningful for active, recently drilled, or recently active wells.
- **Challenge:** We must determine how to handle inactive or historic wells (Spud < 2000) that are near these groups. Should they be absorbed into the modern pad entity, or do they represent a distinct legacy infrastructure layer?
- **Idea:** It might be useful to implement a secondary `Site_ID` based on pure spatial clustering, regardless of company ownership. This would provide a long-term "historical record" for a particular location across different operators and eras.
- **Goal:** Ensure the Master Facility List distinguishes between modern operational clusters and legacy sites to keep downstream resolution accurate.
