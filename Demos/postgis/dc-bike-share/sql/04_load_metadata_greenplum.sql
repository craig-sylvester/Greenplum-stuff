set search_path to dc_bikeshare, public;

DROP TABLE IF EXISTS staging;
CREATE TABLE staging (
  json_data jsonb
)
-- DISTRIBUTED RANDOMLY
;

/***********************************************
  System Information
 ***********************************************/
truncate staging;
insert into staging select * from ext_dcbs_system_info;

truncate system_info;
insert into system_info
select d.*
from staging s,
     jsonb_populate_record(null::system_info, s.json_data->'data') d;

update system_info
     set last_updated = (select to_timestamp ((s.json_data->>'last_updated')::bigint) from staging s);

/***********************************************
  Station Information
 ***********************************************/
truncate staging;
insert into staging select * from ext_dcbs_station_info;

truncate station_info;
insert into station_info
select d.*
from staging s,
     jsonb_populate_recordset(null::station_info, s.json_data->'data'->'stations') d;

update station_info
     set last_updated = (select to_timestamp ((s.json_data->>'last_updated')::bigint) from staging s);

/* Add a geometry point and update from the lon/lat fields */
alter table station_info add column location geometry;
update station_info set location = ST_SETSRID ( ST_MAKEPOINT (lon, lat), 4326 );

/***********************************************
  Station Status
 ***********************************************/
truncate staging;
insert into staging select * from ext_dcbs_station_status;

truncate station_status;
insert into station_status
select d.*
from staging s,
     jsonb_populate_recordset(null::station_status, s.json_data->'data'->'stations') d;

update station_status
     set last_updated = (select to_timestamp ((s.json_data->>'last_updated')::bigint) from staging s);

/***********************************************
  Region Info
 ***********************************************/
truncate staging;
insert into staging select * from ext_dcbs_system_regions;

truncate system_regions;
insert into system_regions
select d.*
from staging s,
     jsonb_populate_recordset(null::system_regions, s.json_data->'data'->'regions') d;

update system_regions
     set last_updated = (select to_timestamp ((s.json_data->>'last_updated')::bigint) from staging s);
