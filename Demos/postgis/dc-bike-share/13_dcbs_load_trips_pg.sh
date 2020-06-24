#!/usr/bin/env bash

source ./dcbikeshare_variables.sh

# Exit on any error or usage of uninitialized variables
set -eu

TBL=trips

DATA_URL='https://s3.amazonaws.com/capitalbikeshare-data'
SUFFIX='-capitalbikeshare-tripdata'

OUTPUT_DIR='./data_trips'
LOAD=false

mkdir -p ${OUTPUT_DIR}


# Check for the existence of the target table
status=$(psql -d ${demo_db} -Atc "select 1 from information_schema.tables where table_schema = '${demo_schema}' and table_name = '${TBL}'")
[[ -z $status  ]] && { echo "Table '${demo_schema}.${TBL}' does not exist." ; exit 1;  }

for year in 2018 2019
do
    for month in $(seq -f '%02g' 1 12)
    do
        DATA_FILE="${year}${month}${SUFFIX}.zip"
        echo "File: ${DATA_FILE}"

        echo "... Downloading"
        wget -q ${DATA_URL}/${DATA_FILE} -O ${OUTPUT_DIR}/${DATA_FILE}

        echo "... Loading"
        unzip -c -p ${OUTPUT_DIR}/${DATA_FILE} | psql -e -d ${demo_db} -c "\copy ${demo_schema}.${TBL} from STDIN CSV HEADER"
    done
done

psql -e -d ${demo_db} -c "analyze ${demo_schema}.${TBL}"
echo "The trips data has been downloaded to the '${OUTPUT_DIR}' directory."
