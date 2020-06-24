#!/usr/bin/env bash

########################################################
#
# Where CLI parameters are: <sub-region> <cache> <processes>
#
# Quickest to load, helpful for testing:
#       ./load_osm.sh colorado 1000 1 &> load_co_osm.log &
#
########################################################
USAGE="$0 <sub-region> <cache size> <processes>"

DB=pgosm

START=$(date +%s)

if [[ $# < 1 ]]; then
    echo $USAGE
    exit 1
fi

REGION=$1
CACHE=1000
PROCESSES=4
[[ ! -z $2 ]] && CACHE=$2
[[ ! -z $3 ]] && PROCESSES=$3

set -u

echo "Start on `hostname` at `date`"
echo "Running Sub-Region:  ${REGION}"

$(psql -At -c 'select datname from pg_database' | grep -w ${DB} &> /dev/null)
if [[ $? == 1 ]]; then
    echo "Creating pgosm DB..."
    psql -d postgres -c "CREATE DATABASE ${DB};"
fi

echo "Creating PostGIS and HSTORE extensions..."
psql -d ${DB} -c "CREATE EXTENSION IF NOT EXISTS postgis; "
psql -d ${DB} -c "CREATE EXTENSION IF NOT EXISTS hstore; "

#echo "Downloading OSM file"
#wget https://download.geofabrik.de/north-america/us/${REGION}-latest.osm.pbf -O ~/tmp/${REGION}-latest.osm.pbf

echo "Starting osm2pgsql for ${REGION}: `date`"
echo "cache: ${CACHE}"
echo "Num processes: ${PROCESSES}"
osm2pgsql \
    --create --slim --drop \
    --cache ${CACHE} \
    --number-processes ${PROCESSES} \
    --hstore \
    --multi-geometry \
    --prefix gis_${REGION} \
    -d ${DB} ./${REGION}-latest.osm.pbf

echo "osm2pgsql completed for ${REGION} on host `hostname` at: `date`"

duration=$(( $(date +%s) - $START ))
echo "Total Duration:  $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
