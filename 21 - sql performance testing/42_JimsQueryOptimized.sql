

-- ============================================================================
--
-- 42_JimsQueryOptimized.sql
--
-- Two-phase geohash-accelerated version of 40_JimsQuery.sql.
--
-- Original problem:
--   ST_DWithin and box2d <-> on the geom column cannot use any index,
--   resulting in a Seq Scan of all 344,688 rows and ~5.2 seconds of
--   Vincenty distance calls.
--
-- Fix:
--   Phase 1 — Use geohash_encode + geohash_neighbors to identify the
--             geohash-8 cells covering the search area.  Filter via
--             geo_hash8 = ANY(ARRAY[...]) which hits ix_mapdata_geo_hash8.
--
--             IMPORTANT: = ANY(ARRAY(...)) produces an Index Scan.
--             IN (SELECT ...) produces a Hash Join -> Seq Scan.
--             The planner treats these differently.
--
--   Phase 2 — Apply the exact Vincenty ST_DWithin only to the small
--             candidate set surviving Phase 1.
--
-- Search point : Fort Collins, CO  (-105.0775, 40.5853)
-- Search radius: 1,000 meters
--
-- Requires: 20_GeohashFunctions.sql  (geohash_encode, geohash_neighbors)
--           12_CreateGeographyType.sql (ST_DWithin geography, ST_Distance)
--           11_CreateSchema.sql        (my_mapdata, ix_mapdata_geo_hash8)
--
-- ============================================================================

EXPLAIN (ANALYZE, VERBOSE, DIST, DEBUG)
WITH nearby_cells AS (
   SELECT unnest(ARRAY[
      -- Center cell
      geohash_encode(40.5853, -105.0775, 8),
      -- 8 surrounding neighbor cells
      (geohash_neighbors(geohash_encode(40.5853, -105.0775, 8))->>'n'),
      (geohash_neighbors(geohash_encode(40.5853, -105.0775, 8))->>'ne'),
      (geohash_neighbors(geohash_encode(40.5853, -105.0775, 8))->>'e'),
      (geohash_neighbors(geohash_encode(40.5853, -105.0775, 8))->>'se'),
      (geohash_neighbors(geohash_encode(40.5853, -105.0775, 8))->>'s'),
      (geohash_neighbors(geohash_encode(40.5853, -105.0775, 8))->>'sw'),
      (geohash_neighbors(geohash_encode(40.5853, -105.0775, 8))->>'w'),
      (geohash_neighbors(geohash_encode(40.5853, -105.0775, 8))->>'nw')
   ]) AS cell_hash
)
SELECT
   md_pk,
   md_name,
   md_address,
   md_city,
   ST_Distance(
      geom::geography,
      ST_SetSRID(ST_MakePoint(-105.0775, 40.5853), 4326)::geography,
      true
   ) AS dist_m
FROM
   my_mapdata
WHERE
   -- Phase 1: geohash index scan (ix_mapdata_geo_hash8)
   geo_hash8 = ANY(ARRAY(SELECT cell_hash FROM nearby_cells))
   -- Phase 2: exact Vincenty refinement on the candidate set
   AND ST_DWithin(
      geom::geography,
      ST_SetSRID(ST_MakePoint(-105.0775, 40.5853), 4326)::geography,
      1000,
      true
   )
ORDER BY
   dist_m
LIMIT 10;



