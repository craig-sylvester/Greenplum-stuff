#!/bin/bash

[ $# -eq 0 ] &&  { echo "Usage: $0 <dbname>"; exit 1 ; }
DB_GIS_ENABLE=$1

# Check if the POSTGIS package has been added before proceeding
gppkg --query --all | grep -i postgis &> /dev/null
[ $? -ne 0 ] && { echo "PostGIS package has not been installed."; exit 1; }

psql -d ${DB_GIS_ENABLE} -f $GPHOME/share/postgresql/contrib/postgis-2.0/postgis.sql
psql -d ${DB_GIS_ENABLE} -f $GPHOME/share/postgresql/contrib/postgis-2.0/spatial_ref_sys.sql
