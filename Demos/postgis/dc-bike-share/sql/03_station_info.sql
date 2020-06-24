SET search_path TO dc_bikeshare;
DROP TABLE IF EXISTS station_info;
CREATE TABLE station_info (
  station_id                      TEXT,
  region_id	                      TEXT,
  rental_methods                  TEXT,  -- this is a json array but just keep as text for now
  eightd_has_key_dispenser        BOOLEAN,
  external_id                     TEXT,
  short_name                      TEXT,
  has_kiosk                       BOOLEAN,
  lat                             FLOAT,
  lon                             FLOAT,
  electric_bike_surcharge_waiver  BOOLEAN,
  name	                          TEXT,
  station_type                    TEXT,
  capacity                        INTEGER,
  last_updated                    TIMESTAMP
)
-- DISTRIBUTED RANDOMLY
;
