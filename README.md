# Geospatial on yugabyteDB — Pure SQL/PL/pgSQL with Geohash Performance

![Screen Shot 1](01%20-%20Images/08%20-%20Screen%20shot%201.png)

![Screen Shot 2](01%20-%20Images/09%20-%20Screen%20shot%202.png)

![GeoServer](01%20-%20Images/10%20-%20GeoServer.png)

---

## Overview

This project delivers a complete geospatial system on **yugabyteDB** using pure SQL and PL/pgSQL — functionally replacing the PostGIS extension. The core innovation is a **two-phase geohash query pattern** that leverages yugabyteDB's B-tree indexes for fast, scalable spatial reads.

yugabyteDB's distributed LSM storage handles B-tree prefix scans on geohash strings naturally — data partitions across tablets cleanly, and performance scales linearly with cluster size. The geohash approach is well suited to a distributed database architecture.

## Getting Started

### 1. Create `properties.ini`

The shell script `20 - sql/60_runAllSQL.sh` reads database connection details from a `properties.ini` file in the project root. Create this file before running the scripts:

```ini
#  Properties file for all files in this folder and below
#
#  The [database] section is required.

[database]
DATABASE_HOST=D1-Yuga-C6N1
DATABASE_PORT=5433
DATABASE_NAME=my_db33
DATABASE_USER=yugabyte
DATABASE_PASSWORD=
```

Adjust the host, port, database name, and user to match your yugabyteDB cluster.

### 2. Extract the Data File

The data load script `20 - sql/15_LoadData.sql` references the pipe-delimited file `19_mapData.pipe`, but the repository contains it in compressed form as `19_mapData.pipe.gz`. Before running the SQL scripts, extract it:

```bash
cd "20 - sql"
gunzip 19_mapData.pipe.gz
```

Alternatively, you can stream the compressed file directly without extracting, by replacing the `\copy` line in `15_LoadData.sql` with a shell pipeline:

```bash
zcat 19_mapData.pipe.gz | ysqlsh \
   -h D1-Yuga-C6N1 -p 5433 -d my_db33 -U yugabyte \
   -c "\copy my_mapdata(md_pk, md_lat, md_lng, geo_hash10, md_name, md_address, md_city, md_province, md_country, md_postcode, md_phone, md_category, md_subcategory, md_mysource, md_tags, md_type) FROM STDIN WITH (FORMAT csv, DELIMITER '|', HEADER true, ROWS_PER_TRANSACTION 100)"
```

### 3. Run All SQL Scripts

From the project root or the `20 - sql` directory:

```bash
bash "20 - sql/60_runAllSQL.sh"
```

This executes all SQL files in order — creating types, the schema, loading data, and installing all geohash and geometry functions.

---

## The Two-Phase Geohash Query Pattern

The key performance technique is a two-phase approach:

1. **Phase 1 — Geohash pre-filter (index scan):** Convert the search area into geohash cell prefixes and use a B-tree index lookup to narrow the candidate set.
2. **Phase 2 — Exact spatial refinement:** Apply precise distance or containment functions only on the small candidate set returned by Phase 1.

This produces an **Index Scan** on the geohash B-tree index, scanning only the relevant rows instead of the entire table.

### The Critical SQL Pattern

```sql
-- Two-phase geohash query — produces Index Scan on ix_mapdata3
SELECT md_pk, md_name, md_address, md_city, geom
FROM my_mapdata
WHERE
   LEFT(geo_hash10, 5) = ANY(
      ARRAY(SELECT geohash_cells_for_bbox(lon_min, lat_min, lon_max, lat_max, 5))
   )
```

The `= ANY(ARRAY(...))` syntax is essential — the query planner treats this as an index-scannable equality condition. The `geohash_cells_for_bbox` function computes which geohash cells overlap the bounding box, and the B-tree index `ix_mapdata3` on `LEFT(geo_hash10, 5)` delivers the matching rows directly.

---

## Configuring GeoServer with a SQL View for Fast Indexed Reads

GeoServer can be configured to use this two-phase geohash pattern through a **SQL View** layer. Instead of querying the table directly (which would require a GiST index that yugabyteDB does not use), the SQL View injects the geohash pre-filter so that every bounding-box request from GeoServer hits the B-tree index.

### Prerequisites

- GeoServer started with JSONP enabled:
  ```
  export JAVA_OPTS="${JAVA_OPTS} -DENABLE_JSONP=true"
  ```
- The CORS filter in `web.xml` uncommented (Jetty section)

### Step-by-Step GeoServer Configuration

**1. Create Workspace**

- Left panel → Workspaces → Add new workspace
  - Name: `yugabyte`
  - URI: `http://yugabyte.local`
  - Default workspace: checked

**2. Create Store**

- Left panel → Stores → Add new store → PostGIS
  - Workspace: `yugabyte`
  - Data Source Name: `my_db33`
  - host: `D1-Yuga-C6N1`
  - port: `5433`
  - database: `my_db33`
  - schema: `public`
  - user: `yugabyte`
  - Expose primary keys: checked
  - Estimated extends: checked
  - Encode functions: checked
  - Loose bbox: checked
  - preparedStatements: unchecked

**3. Create the SQL View Layer**

- Left panel → Layers → Add a new layer
- Select `yugabyte:my_db33` → click **"Configure new SQL view..."**

**4. View Name:** `my_mapdata_fast`

**5. SQL Statement** (paste this, no trailing semicolon):

```sql
SELECT
   md_pk,
   md_name,
   md_address,
   md_city,
   md_province,
   md_postcode,
   md_category,
   md_subcategory,
   geom
FROM
   my_mapdata
WHERE
   LEFT(geo_hash10, 5) = ANY(
      ARRAY(SELECT geohash_cells_for_bbox(
         cast(%LON_MIN% as numeric),
         cast(%LAT_MIN% as numeric),
         cast(%LON_MAX% as numeric),
         cast(%LAT_MAX% as numeric),
         5
      ))
   )
```

The `cast(%PARAM% as numeric)` wrappers are required because GeoServer types its viewparams as `numeric`.

**6. Guess Parameters from SQL**

Click **"Guess parameters from SQL"**. Four parameters appear. Configure them:

| Name    | Default Value | Validation (regex) |
|---------|---------------|--------------------|
| LON_MIN | -105.09       | `^-?[\d.]+$`       |
| LAT_MIN | 40.57         | `^-?[\d.]+$`       |
| LON_MAX | -105.06       | `^-?[\d.]+$`       |
| LAT_MAX | 40.60         | `^-?[\d.]+$`       |

**7. Refresh Attributes**

Click **"Refresh"** next to "Attributes and types" to detect columns. Ensure:
- `geom` is recognized as Geometry, type Point, SRID 4326
- `md_pk` is the identifier

**8. Save and Publish**

- Click Save, then publish the layer
- Under "Bounding Boxes": click "Compute from data", then "Compute from native bounds"

**9. Test**

Test via Layer Preview → OpenLayers, or paste this URL in a browser:

```
http://localhost:8080/geoserver/yugabyte/wfs?service=WFS&version=1.0.0
   &request=GetFeature
   &typeName=yugabyte:my_mapdata_fast
   &outputFormat=application/json
   &maxFeatures=5
   &viewparams=LON_MIN:-105.09;LAT_MIN:40.57;LON_MAX:-105.06;LAT_MAX:40.60
```

### How the Client Passes the Bounding Box

The client passes the bounding box via the `viewparams` parameter in the WFS URL:

```
&viewparams=LON_MIN:-105.09;LAT_MIN:40.57;LON_MAX:-105.06;LAT_MAX:40.60
```

GeoServer substitutes these into the SQL View query before execution. CQL filters (e.g., `md_name ILIKE '%Starbucks%'`) are appended as additional WHERE conditions on top of the geohash pre-filter, so they only run against the reduced candidate set.

### Geohash Precision and Performance

Precision 5 is used because each cell is approximately 2.4 miles wide, mapping directly to the `ix_mapdata3` index on `LEFT(geo_hash10, 5)`:

| Bounding Box Size | Approximate Cells | Performance    |
|--------------------|-------------------|----------------|
| ~1 mile            | ~4 cells          | Sub-second     |
| ~5 miles           | ~9 cells          | Fast           |
| ~25 miles          | ~100 cells        | Moderate       |
| ~150 miles         | ~3,600 cells      | Multiple seconds |

For tighter queries (under 1 mile), the auto-precision overload of `geohash_cells_for_bbox` (no precision argument) can be used — it auto-selects precision 5, 6, or 8 based on bbox size.

### Verification

Test the query directly in ysqlsh to confirm it uses the index:

```sql
EXPLAIN (ANALYZE, VERBOSE, DIST)
SELECT
   md_pk, md_name, md_address, md_city, geom
FROM
   my_mapdata
WHERE
   LEFT(geo_hash10, 5) = ANY(
      ARRAY(SELECT geohash_cells_for_bbox(-105.09, 40.57, -105.06, 40.60, 5))
   );
```

Expected plan: **Index Scan using ix_mapdata3** on my_mapdata (~64ms, ~3,140 rows scanned).

---

## Project Architecture

### SQL Objects

| Type        | Count | Purpose                                                                 |
|-------------|-------|-------------------------------------------------------------------------|
| TYPE        | 3     | `geometry`, `box2d`, `geography` — composite types for spatial data     |
| CAST        | 5     | Implicit conversions between geometry, geography, and box2d             |
| OPERATOR    | 5     | `&&` (bbox overlap), `<->` (KNN distance) for geometry and geography   |
| AGGREGATE   | 2     | `ST_Extent` — bounding box across all rows (required by GeoServer)     |
| TABLE       | 2     | `my_mapdata` (~344,688 Colorado POI records), `spatial_ref_sys`        |
| VIEW        | 2     | `geometry_columns`, `geography_columns` — GeoServer metadata discovery |
| INDEX       | 4     | B-tree indexes on geohash prefixes at precisions 5, 6, 8, and 10      |
| FUNCTION    | 160+  | Full PostGIS-compatible function library in pure PL/pgSQL              |

### Function Categories

| Group | Category                          | Examples                                                    |
|-------|-----------------------------------|-------------------------------------------------------------|
| A     | Internal Helpers                  | Binary encoding, cast helpers, operator backing functions    |
| B     | GeoServer Compatibility Shims     | `PostGIS_Version`, `ST_SRID`, `ST_SetSRID`, `ST_Transform`  |
| C     | Constructors & Parsers            | `ST_MakePoint`, `ST_GeomFromText`, `ST_GeomFromGeoJSON`     |
| D     | Spatial Predicates                | `ST_Intersects`, `ST_Contains`, `ST_DWithin`, `ST_Within`   |
| E     | Distance & Measurement            | `ST_Distance`, `ST_DistanceSphere`, `ST_DistanceSpheroid`   |
| F     | Accessors & Property Inspectors   | `ST_X`, `ST_Y`, `ST_NPoints`, `GeometryType`, `ST_IsValid`  |
| G     | Output/Serialization              | `ST_AsText`, `ST_AsGeoJSON`, `ST_AsBinary`, `ST_AsTWKB`     |
| H     | Geometry Manipulation             | `ST_Centroid`, `ST_Buffer`, `ST_Union`, `ST_Intersection`    |
| I     | Geohash Utilities                 | `geohash_encode`, `geohash_adjacent`, `geohash_cells_for_bbox` |
| J     | Set-Returning / Utility           | `ST_DumpPoints`, `ST_DumpSegments`, `ST_GeneratePoints`      |

### Spatial Models Supported

- **Planar** — standard Euclidean operations on degree coordinates
- **Spherical** — Haversine-based calculations (meters)
- **Spheroid** — Vincenty-based calculations (meters, WGS-84 ellipsoid)

### Indexes

| Index                  | Expression                    | Use Case                                    |
|------------------------|-------------------------------|---------------------------------------------|
| `ix_my_mapdata2`       | `(geo_hash10, md_name)`       | Full-precision geohash lookups              |
| `ix_mapdata3`          | `(LEFT(geo_hash10, 5), md_name)` | ~150-mile radius queries (GeoServer SQL View) |
| `ix_mapdata4`          | `(LEFT(geo_hash10, 6), md_name)` | ~20-mile radius, local-search use cases     |
| `ix_mapdata_geo_hash8` | `(geo_hash8)`                 | Precision-8 geohash equality, polygon coverage |

### Performance Comparison

| Approach                       | Execution Time | Rows Scanned | Plan            |
|--------------------------------|----------------|--------------|-----------------|
| `= ANY(ARRAY(SELECT ...))` | ~64 ms         | ~3,140       | **Index Scan**  |
| Two-phase with CTE + `geo_hash8` | ~5 ms       | ~0 (cache)   | **Index Scan**  |

### Query Plan Approach

Queries that use geohash indexes well:
- Any query that pre-computes geohash cells and uses `WHERE LEFT(geo_hash10, N) = ...` or `WHERE geo_hash8 = ...` — these become B-tree equality/prefix lookups, which yugabyteDB's LSM storage handles well across distributed tablets.

The two-phase rewrite pattern works for all spatial query types:
- **Circle queries** — convert center + radius to bounding box, geohash pre-filter, then Haversine/Vincenty refinement
- **Box queries** — direct bounding box to geohash cells
- **Polygon queries** — bounding box of polygon to geohash cells, then point-in-polygon refinement

### The Geohash Advantage on Distributed Databases

B-tree prefix scans on geohash strings partition and scale across tablets naturally. The geohash approach is well suited to distributed database architectures — data for nearby geographic locations shares a common prefix, so range scans are efficient and tablet-local. This is a strong pattern for yugabyteDB's distributed storage layer.
         cast(%LON_MIN% as numeric),
         cast(%LAT_MIN% as numeric),
         cast(%LON_MAX% as numeric),
         cast(%LAT_MAX% as numeric),
         5
      ))
   )
```

The `cast(%PARAM% as numeric)` wrappers are required because GeoServer types its viewparams as `numeric`.

**6. Guess Parameters from SQL**

Click **"Guess parameters from SQL"**. Four parameters appear. Configure them:

| Name    | Default Value | Validation (regex) |
|---------|---------------|--------------------|
| LON_MIN | -105.09       | `^-?[\d.]+$`       |
| LAT_MIN | 40.57         | `^-?[\d.]+$`       |
| LON_MAX | -105.06       | `^-?[\d.]+$`       |
| LAT_MAX | 40.60         | `^-?[\d.]+$`       |

**7. Refresh Attributes**

Click **"Refresh"** next to "Attributes and types" to detect columns. Ensure:
- `geom` is recognized as Geometry, type Point, SRID 4326
- `md_pk` is the identifier

**8. Save and Publish**

- Click Save, then publish the layer
- Under "Bounding Boxes": click "Compute from data", then "Compute from native bounds"

**9. Test**

Test via Layer Preview → OpenLayers, or paste this URL in a browser:

```
http://localhost:8080/geoserver/yugabyte/wfs?service=WFS&version=1.0.0
   &request=GetFeature
   &typeName=yugabyte:my_mapdata_fast
   &outputFormat=application/json
   &maxFeatures=5
   &viewparams=LON_MIN:-105.09;LAT_MIN:40.57;LON_MAX:-105.06;LAT_MAX:40.60
```

### How the Client Passes the Bounding Box

The client passes the bounding box via the `viewparams` parameter in the WFS URL:

```
&viewparams=LON_MIN:-105.09;LAT_MIN:40.57;LON_MAX:-105.06;LAT_MAX:40.60
```

GeoServer substitutes these into the SQL View query before execution. CQL filters (e.g., `md_name ILIKE '%Starbucks%'`) are appended as additional WHERE conditions on top of the geohash pre-filter, so they only run against the reduced candidate set.

### Geohash Precision and Performance

Precision 5 is used because each cell is approximately 2.4 miles wide, mapping directly to the `ix_mapdata3` index on `LEFT(geo_hash10, 5)`:

| Bounding Box Size | Approximate Cells | Performance    |
|--------------------|-------------------|----------------|
| ~1 mile            | ~4 cells          | Sub-second     |
| ~5 miles           | ~9 cells          | Fast           |
| ~25 miles          | ~100 cells        | Moderate       |
| ~150 miles         | ~3,600 cells      | Multiple seconds |

For tighter queries (under 1 mile), the auto-precision overload of `geohash_cells_for_bbox` (no precision argument) can be used — it auto-selects precision 5, 6, or 8 based on bbox size.

### Verification

Test the query directly in ysqlsh to confirm it uses the index:

```sql
EXPLAIN (ANALYZE, VERBOSE, DIST)
SELECT
   md_pk, md_name, md_address, md_city, geom
FROM
   my_mapdata
WHERE
   LEFT(geo_hash10, 5) = ANY(
      ARRAY(SELECT geohash_cells_for_bbox(-105.09, 40.57, -105.06, 40.60, 5))
   );
```

Expected plan: **Index Scan using ix_mapdata3** on my_mapdata (~64ms, ~3,140 rows scanned).

---

## Project Architecture

### SQL Objects

| Type        | Count | Purpose                                                                 |
|-------------|-------|-------------------------------------------------------------------------|
| TYPE        | 3     | `geometry`, `box2d`, `geography` — composite types for spatial data     |
| CAST        | 5     | Implicit conversions between geometry, geography, and box2d             |
| OPERATOR    | 5     | `&&` (bbox overlap), `<->` (KNN distance) for geometry and geography   |
| AGGREGATE   | 2     | `ST_Extent` — bounding box across all rows (required by GeoServer)     |
| TABLE       | 2     | `my_mapdata` (~344,688 Colorado POI records), `spatial_ref_sys`        |
| VIEW        | 2     | `geometry_columns`, `geography_columns` — GeoServer metadata discovery |
| INDEX       | 4     | B-tree indexes on geohash prefixes at precisions 5, 6, 8, and 10      |
| FUNCTION    | 160+  | Full PostGIS-compatible function library in pure PL/pgSQL              |

### Function Categories

| Group | Category                          | Examples                                                    |
|-------|-----------------------------------|-------------------------------------------------------------|
| A     | Internal Helpers                  | Binary encoding, cast helpers, operator backing functions    |
| B     | GeoServer Compatibility Shims     | `PostGIS_Version`, `ST_SRID`, `ST_SetSRID`, `ST_Transform`  |
| C     | Constructors & Parsers            | `ST_MakePoint`, `ST_GeomFromText`, `ST_GeomFromGeoJSON`     |
| D     | Spatial Predicates                | `ST_Intersects`, `ST_Contains`, `ST_DWithin`, `ST_Within`   |
| E     | Distance & Measurement            | `ST_Distance`, `ST_DistanceSphere`, `ST_DistanceSpheroid`   |
| F     | Accessors & Property Inspectors   | `ST_X`, `ST_Y`, `ST_NPoints`, `GeometryType`, `ST_IsValid`  |
| G     | Output/Serialization              | `ST_AsText`, `ST_AsGeoJSON`, `ST_AsBinary`, `ST_AsTWKB`     |
| H     | Geometry Manipulation             | `ST_Centroid`, `ST_Buffer`, `ST_Union`, `ST_Intersection`    |
| I     | Geohash Utilities                 | `geohash_encode`, `geohash_adjacent`, `geohash_cells_for_bbox` |
| J     | Set-Returning / Utility           | `ST_DumpPoints`, `ST_DumpSegments`, `ST_GeneratePoints`      |

### Spatial Models Supported

- **Planar** — standard Euclidean operations on degree coordinates
- **Spherical** — Haversine-based calculations (meters)
- **Spheroid** — Vincenty-based calculations (meters, WGS-84 ellipsoid)

### Indexes

| Index                  | Expression                    | Use Case                                    |
|------------------------|-------------------------------|---------------------------------------------|
| `ix_my_mapdata2`       | `(geo_hash10, md_name)`       | Full-precision geohash lookups              |
| `ix_mapdata3`          | `(LEFT(geo_hash10, 5), md_name)` | ~150-mile radius queries (GeoServer SQL View) |
| `ix_mapdata4`          | `(LEFT(geo_hash10, 6), md_name)` | ~20-mile radius, local-search use cases     |
| `ix_mapdata_geo_hash8` | `(geo_hash8)`                 | Precision-8 geohash equality, polygon coverage |

### Performance Comparison

| Approach                       | Execution Time | Rows Scanned | Plan            |
|--------------------------------|----------------|--------------|-----------------|
| `= ANY(ARRAY(SELECT ...))` | ~64 ms         | ~3,140       | **Index Scan**  |
| Two-phase with CTE + `geo_hash8` | ~5 ms       | ~0 (cache)   | **Index Scan**  |

### Query Plan Approach

Queries that use geohash indexes well:
- Any query that pre-computes geohash cells and uses `WHERE LEFT(geo_hash10, N) = ...` or `WHERE geo_hash8 = ...` — these become B-tree equality/prefix lookups, which yugabyteDB's LSM storage handles well across distributed tablets.

The two-phase rewrite pattern works for all spatial query types:
- **Circle queries** — convert center + radius to bounding box, geohash pre-filter, then Haversine/Vincenty refinement
- **Box queries** — direct bounding box to geohash cells
- **Polygon queries** — bounding box of polygon to geohash cells, then point-in-polygon refinement

### The Geohash Advantage on Distributed Databases

B-tree prefix scans on geohash strings partition and scale across tablets naturally. The geohash approach is well suited to distributed database architectures — data for nearby geographic locations shares a common prefix, so range scans are efficient and tablet-local. This is a strong pattern for yugabyteDB's distributed storage layer.
