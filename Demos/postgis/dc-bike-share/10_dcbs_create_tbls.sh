#!/usr/bin/env bash

source ./dcbikeshare_variables.sh

set -eu

psql -e -d ${demo_db} -c "create schema ${demo_schema}"

for SQL_FILE in sql/03*.sql
do
    psql -e -d ${demo_db} -f ${SQL_FILE}
done

if [[ ${database} == "greenplum" ]]; then
    FILE='./sql/02_greenplum_ext_tbls.sql'
    [[ -r ${FILE} ]] && psql -e -d ${demo_db} -f ${FILE} || { echo "'${FILE}' not found"; exit 1; }
else
    FILE='./sql/02_fdw.sql'

    [[ -r ${FILE} ]] && psql -e -d ${demo_db} -f ${FILE} || { echo "'${FILE}' not found"; exit 1; }

    cat << EOF

###########################################################################################
A PostgreSQL superuser must create the staging table we will use to import the metadata.
Execute the following as a PG superuser before moving to the next step:

 CREATE FOREIGN TABLE ${demo_schema}.staging ( json_data jsonb )
 SERVER json_file
 OPTIONS (FILENAME '/tmp/dc_bikeshare_metadata.json', FORMAT 'text')
 ;

 GRANT SELECT ON ${demo_schema}.staging to ${demo_dbuser};
###########################################################################################

EOF

fi
