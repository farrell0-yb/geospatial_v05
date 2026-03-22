-- ============================================================================
--
-- 31_GeohashBboxFunctions.sql
--
-- Purpose  : Convert a bounding box (lon_min, lat_min, lon_max, lat_max) into
--            the set of geohash cells that cover it.  This bridges the gap
--            between GeoServer's BBOX filter and our geohash indexes.
--
-- Requires : 20_GeohashFunctions.sql  (geohash_encode, geohash_adjacent,
--                                       geohash_decode_bbox,
--                                       geohash_precision_for_miles)
--
-- What you get:
--   1) geohash_cells_for_bbox(lon_min, lat_min, lon_max, lat_max, precision)
--      -> SETOF text
--      Returns every geohash cell at the given precision that intersects
--      the bounding box.
--
--   2) geohash_cells_for_bbox(lon_min, lat_min, lon_max, lat_max)
--      -> SETOF text
--      Auto-selects precision based on bbox size so the cell count stays
--      manageable.  Maps to the existing geohash indexes:
--         precision 5 -> ix_mapdata3  (LEFT(geo_hash10, 5))
--         precision 6 -> ix_mapdata4  (LEFT(geo_hash10, 6))
--         precision 8 -> ix_mapdata_geo_hash8
--
--   3) Numeric overloads for GeoServer compatibility (passes numeric args).
--
-- ============================================================================


-- ------------------------------------------------------------
-- 1) Enumerate geohash cells covering a bounding box
--    (explicit precision)
-- ------------------------------------------------------------
--
-- Walks from the SW corner to the NE corner of the bbox, emitting every
-- geohash cell that the box touches.  Uses the geohash grid itself for
-- stepping, so it works at any precision (1-10).
--
-- Example:
--   SELECT * FROM geohash_cells_for_bbox(-105.09, 40.57, -105.06, 40.60, 6);
--   -- Returns: '9xj6r0', '9xj6r1', ... (all precision-6 cells in the box)
--
CREATE OR REPLACE FUNCTION geohash_cells_for_bbox(
   p_lon_min double precision,
   p_lat_min double precision,
   p_lon_max double precision,
   p_lat_max double precision,
   p_precision integer
)
RETURNS SETOF text
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
   sw_hash    text;
   row_start  text;
   cur        text;
   cur_bbox   record;
BEGIN
   -- Encode the SW corner to get our starting cell
   sw_hash := geohash_encode(p_lat_min, p_lon_min, p_precision);

   -- Walk rows south-to-north
   row_start := sw_hash;
   LOOP
      -- Walk columns west-to-east within this row
      cur := row_start;
      LOOP
         RETURN NEXT cur;

         -- Step east
         cur := geohash_adjacent(cur, 'e');
         SELECT * INTO cur_bbox FROM geohash_decode_bbox(cur);

         -- If this cell's west edge is past our east boundary, row is done
         EXIT WHEN cur_bbox.lon_min >= p_lon_max;
      END LOOP;

      -- Step north to the next row
      row_start := geohash_adjacent(row_start, 'n');
      SELECT * INTO cur_bbox FROM geohash_decode_bbox(row_start);

      -- If this row's south edge is past our north boundary, we're done
      EXIT WHEN cur_bbox.lat_min >= p_lat_max;
   END LOOP;
END;
$$;


-- ------------------------------------------------------------
-- 2) Auto-precision version (no precision argument)
-- ------------------------------------------------------------
--
-- Chooses precision based on the longer dimension of the bbox in miles,
-- capped to precisions that have matching indexes (5, 6, or 8).
--
--   bbox span > 20 miles  -> precision 5  (uses ix_mapdata3)
--   bbox span > 1 mile    -> precision 6  (uses ix_mapdata4)
--   bbox span <= 1 mile   -> precision 8  (uses ix_mapdata_geo_hash8)
--
-- Example:
--   SELECT * FROM geohash_cells_for_bbox(-105.55, 40.22, -104.60, 40.95);
--   -- Auto-selects precision 5 (~50 mile span)
--
CREATE OR REPLACE FUNCTION geohash_cells_for_bbox(
   p_lon_min double precision,
   p_lat_min double precision,
   p_lon_max double precision,
   p_lat_max double precision
)
RETURNS SETOF text
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
   lat_span_miles double precision;
   lon_span_miles double precision;
   max_span       double precision;
   prec           integer;
BEGIN
   -- Approximate bbox span in miles
   -- 1 degree latitude  ~ 69 miles
   -- 1 degree longitude ~ 69 * cos(mid_lat) miles
   lat_span_miles := abs(p_lat_max - p_lat_min) * 69.0;
   lon_span_miles := abs(p_lon_max - p_lon_min) * 69.0
                     * cos(radians((p_lat_min + p_lat_max) / 2.0));
   max_span := greatest(lat_span_miles, lon_span_miles);

   -- Pick precision to match available indexes
   IF max_span > 20.0 THEN
      prec := 5;    -- ix_mapdata3: LEFT(geo_hash10, 5)
   ELSIF max_span > 1.0 THEN
      prec := 6;    -- ix_mapdata4: LEFT(geo_hash10, 6)
   ELSE
      prec := 8;    -- ix_mapdata_geo_hash8
   END IF;

   RETURN QUERY SELECT geohash_cells_for_bbox(
      p_lon_min, p_lat_min, p_lon_max, p_lat_max, prec
   );
END;
$$;


-- ------------------------------------------------------------
-- 3) Numeric overloads (GeoServer compatibility)
-- ------------------------------------------------------------
--
-- GeoServer substitutes SQL View parameters as numeric literals.
-- These overloads accept numeric and delegate to double precision.
--
CREATE OR REPLACE FUNCTION geohash_cells_for_bbox(
   p_lon_min numeric,
   p_lat_min numeric,
   p_lon_max numeric,
   p_lat_max numeric,
   p_precision integer
)
RETURNS SETOF text
LANGUAGE sql IMMUTABLE
AS $$
   SELECT geohash_cells_for_bbox(
      p_lon_min::double precision,
      p_lat_min::double precision,
      p_lon_max::double precision,
      p_lat_max::double precision,
      p_precision
   );
$$;

CREATE OR REPLACE FUNCTION geohash_cells_for_bbox(
   p_lon_min numeric,
   p_lat_min numeric,
   p_lon_max numeric,
   p_lat_max numeric
)
RETURNS SETOF text
LANGUAGE sql IMMUTABLE
AS $$
   SELECT geohash_cells_for_bbox(
      p_lon_min::double precision,
      p_lat_min::double precision,
      p_lon_max::double precision,
      p_lat_max::double precision
   );
$$;
