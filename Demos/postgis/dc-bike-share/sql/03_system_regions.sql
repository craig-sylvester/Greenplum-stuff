SET search_path TO dc_bikeshare;
DROP TABLE IF EXISTS system_regions;
CREATE TABLE system_regions (
  region_id    TEXT,
  name	       TEXT,
  last_updated TIMESTAMP
)
-- DISTRIBUTED RANDOMLY
;
