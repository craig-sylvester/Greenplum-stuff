#!/usr/bin/env bash

source ./dcbikeshare_variables.sh

# Let's make sure we are running against a Greenplum database.
[[ ${database} == "postgres" ]] && { echo "PXF+minio not used for PostgreSQL"; exit 0; }

DB=${PGDATABASE:-${USER}}
SCHEMA='public'
EXT_TBL="ext_minio_station_info"

# Create the Greenplum external table
psql -e -d ${DB} << EOF
DROP EXTERNAL TABLE IF EXISTS ${SCHEMA}.${EXT_TBL};
CREATE EXTERNAL TABLE ${SCHEMA}.${EXT_TBL} (
   station_id           text,
   name                 text,
   capacity             integer,
   has_kiosk            boolean,
   "rental_uris.ios"    text,
   "rental_methods[0]"  text,
   "rental_methods[1]"  text
)
   LOCATION ('pxf://data/station_information.json?PROFILE=s3:json&SERVER=minio&IDENTIFIER=station_id')
   FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
;
EOF

psql -e -d ${DB} -c "select * from ${SCHEMA}.${EXT_TBL} limit 10"
