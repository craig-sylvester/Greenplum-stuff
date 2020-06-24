SET search_path TO dc_bikeshare;
DROP TABLE IF EXISTS system_info;
CREATE TABLE system_info (
  name          TEXT,
  start_date    TEXT,
  timezone      TEXT,
  email	        TEXT,
  language      TEXT,
  license_url   TEXT,
  system_id     TEXT,
  short_name    TEXT,
  operator      TEXT,
  url           TEXT,
  purchase_url  TEXT,
  phone_number  TEXT,
  last_updated  TIMESTAMP
)
-- DISTRIBUTED RANDOMLY
;
