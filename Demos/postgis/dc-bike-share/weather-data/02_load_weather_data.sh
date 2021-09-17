#!/usr/bin/env bash

set -ue

SCHEMA="dc_bikeshare"
BASE_NAME="dc_weather"
LOAD_TABLE="${BASE_NAME}_json"
TABLE="${BASE_NAME}"
VIEW="v_${BASE_NAME}"

# Input file to use without any extensions.
# For our purposes below, we assume an extension of '.json' for the ingest file and will
# output to a file with a '.dat' extension.

WD_FILE="dca_hourly_2018_2019"

echo "Using 'jq', convert the incoming file containing 1 large JSON array to a format of"
echo "one JSON document per line."

echo "jq -c '.data[]' ${WD_FILE}.json > ${WD_FILE}.dat"
jq -c '.data[]' ${WD_FILE}.json > ${WD_FILE}.dat

echo "Create a table for our weather data"
psql -e -h ${PGHOST:-127.0.0.1} -U ${PGUSER:-$USER} -d ${PGDATABASE:-$USER} << EOF
DROP VIEW IF EXISTS ${SCHEMA}.${VIEW};
DROP TABLE IF EXISTS ${SCHEMA}.${LOAD_TABLE} ;
CREATE TABLE ${SCHEMA}.${LOAD_TABLE} (
   id              serial,
   hourly_weather  jsonb
)
;

COMMENT ON TABLE ${SCHEMA}.${LOAD_TABLE} IS 'Data provided by "https://www.meteostat.net"';
EOF

echo "Load the weather data to our newly created table"
psql -e -h ${PGHOST:-127.0.0.1} -U ${PGUSER:-$USER} -d ${PGDATABASE:-$USER} -c "\copy ${SCHEMA}.${LOAD_TABLE} (hourly_weather) from './${WD_FILE}.dat'"

echo << EOF
Create a VIEW to make it easier to query the data.
In addition, a database table is created from the view
for comparing runtimes in one example query.
EOF

psql -e -h ${PGHOST:-127.0.0.1} -U ${PGUSER:-$USER} -d ${PGDATABASE:-$USER} << EOF
DROP VIEW IF EXISTS ${SCHEMA}.${VIEW} ;
CREATE VIEW ${SCHEMA}.${VIEW} AS
SELECT
   cast (hourly_weather->>'time' as timestamp)          as time_local,
   cast (hourly_weather->>'temp' as float)              as temp_celsius,
   cast (hourly_weather->>'temp' as float) * 1.8 + 32   as temp_fahrenheit,
   cast (hourly_weather->>'prcp' as float)              as precipitation_mm,
   cast (hourly_weather->>'wspd' as float)              as windspeed_km_per_hr,
   cast (hourly_weather->>'snow' as float)              as snowdepth_mm,
   cast (hourly_weather->>'rhum' as int)                as humidity
FROM ${SCHEMA}.${LOAD_TABLE}
;

DROP TABLE IF EXISTS ${SCHEMA}.${TABLE};
CREATE TABLE ${SCHEMA}.${TABLE} AS SELECT * from ${SCHEMA}.${VIEW}
-- DISTRIBUTED RANDOMLY
;

ANALYZE ${SCHEMA}.${TABLE};

EOF
