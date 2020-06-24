#!/usr/bin/env bash

source ./dcbikeshare_variables.sh

set -eu

echo "Load the configuration and status tables into the DC BikeShare (dc_bikeshare) schema."

if [[ ${database} == "greenplum" ]]; then
    SQL='sql/04_load_metadata_greenplum.sql'
else
    SQL='sql/04_load_metadata_postgres.sql'
fi

psql -d ${demo_db} -ef ${SQL}
