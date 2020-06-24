SET search_path TO dc_bikeshare;
DROP TABLE IF EXISTS trips;
CREATE TABLE trips (
    duration          INT,
    start_date        TIMESTAMP,
    end_date          TIMESTAMP,
    start_station_num INT,
    start_station     TEXT,
    end_station_num   INT,
    end_station       TEXT,
    bike_num          TEXT,
    member_type       TEXT
)
-- DISTRIBUTED RANDOMLY
;
