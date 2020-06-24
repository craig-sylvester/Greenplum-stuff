set search_path to dc_bikeshare, public;

/* vvvv This part must be run by a user with superuser privs
DROP FOREIGN TABLE IF EXISTS dc_bikeshare.staging;
CREATE FOREIGN TABLE dc_bikeshare.staging (
  json_data jsonb
)
SERVER json_file
OPTIONS (FILENAME '/tmp/dc_bikeshare_metadata.json', FORMAT 'text')
;
GRANT SELECT on dc_bikeshare.staging to <user>;

   ^^^^ */

/***********************************************
  System Information
 ***********************************************/
\! /tmp/dcbikeshare_get_metadata.sh system_information > /tmp/dc_bikeshare_metadata.json

truncate table system_info;

insert into system_info
select d.*
from staging s,
     jsonb_populate_record(null::system_info, s.json_data->'data') d;

update system_info
     set last_updated = (select to_timestamp ((s.json_data->>'last_updated')::bigint) from staging s);

/***********************************************
  Station Information
 ***********************************************/
\! /tmp/dcbikeshare_get_metadata.sh station_information > /tmp/dc_bikeshare_metadata.json

truncate table station_info;

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
\! /tmp/dcbikeshare_get_metadata.sh station_status > /tmp/dc_bikeshare_metadata.json

truncate table station_status;

insert into station_status
select d.*
from staging s,
     jsonb_populate_recordset(null::station_status, s.json_data->'data'->'stations') d;

update station_status
     set last_updated = (select to_timestamp ((s.json_data->>'last_updated')::bigint) from staging s);

/***********************************************
  Region Info
 ***********************************************/
\! /tmp/dcbikeshare_get_metadata.sh system_regions > /tmp/dc_bikeshare_metadata.json

truncate table system_regions;

insert into system_regions
select d.*
from staging s,
     jsonb_populate_recordset(null::system_regions, s.json_data->'data'->'regions') d;

update system_regions
     set last_updated = (select to_timestamp ((s.json_data->>'last_updated')::bigint) from staging s);
