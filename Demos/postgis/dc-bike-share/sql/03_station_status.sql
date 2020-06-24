SET search_path TO dc_bikeshare;
DROP TABLE IF EXISTS station_status;
CREATE TABLE station_status (
  station_id                 TEXT,
  station_status             TEXT,
  num_bikes_available        INTEGER,
  num_bikes_disabled         INTEGER,
  num_ebikes_available       INTEGER,
  num_docks_available        INTEGER,
  num_docks_disabled         INTEGER,
  is_renting                 BOOLEAN,
  is_returning               BOOLEAN,
  is_installed               BOOLEAN,
  eightd_has_available_keys  BOOLEAN,
  last_updated               TIMESTAMP
)
-- DISTRIBUTED RANDOMLY
;
