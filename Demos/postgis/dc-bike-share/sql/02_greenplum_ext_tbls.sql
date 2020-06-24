SET SEARCH_PATH to dc_bikeshare, public;

create extension pxf;

DROP EXTERNAL WEB TABLE IF EXISTS ext_dcbs_system_info;
CREATE EXTERNAL WEB TABLE ext_dcbs_system_info ( data jsonb)
    EXECUTE '/tmp/dcbikeshare_get_metadata.sh system_information' ON MASTER FORMAT 'text'
;

DROP EXTERNAL WEB TABLE IF EXISTS ext_dcbs_station_info;
CREATE EXTERNAL WEB TABLE ext_dcbs_station_info ( data jsonb)
    EXECUTE '/tmp/dcbikeshare_get_metadata.sh station_information' ON MASTER FORMAT 'text'
;

DROP EXTERNAL WEB TABLE IF EXISTS ext_dcbs_station_status;
CREATE EXTERNAL WEB TABLE ext_dcbs_station_status ( data jsonb)
    EXECUTE '/tmp/dcbikeshare_get_metadata.sh station_status' ON MASTER FORMAT 'text'
;

DROP EXTERNAL WEB TABLE IF EXISTS ext_dcbs_system_regions;
CREATE EXTERNAL WEB TABLE ext_dcbs_system_regions ( data jsonb)
    EXECUTE '/tmp/dcbikeshare_get_metadata.sh system_regions' ON MASTER FORMAT 'text'
;
