#!/usr/bin/env bash

set -u

if [[ ${USER} != "gpadmin" ]] && [[ ${USER} != "pivotal" ]]; then
    echo "This script must be run by gpadmin or pivotal"
    exit 0
fi

cat << EOF
Welcome to the DC Bike Share demo for PostgreSQL and Greenplum!
The scripts contained in the directory will set up a schema and set of tables
in a PostgreSQL/Greenplum database. There is an accompanying Jupyter notebook
with sample queries, charts, and maps.

Before starting, let's do a bit of setup:
EOF
read -p "Database to use for the demo (default = $USER): " ans
DatabaseName=${ans:-$USER}
read -p "Schema to use for the demo (default = dc_bikeshare): " ans
SchemaName=${ans:-dc_bikeshare}
read -p "DB user to use for the demo (default = $USER): " ans
DbUserName=${ans:-$USER}

#########################################################################
# Create and start populating a settings file. We will store common
# variables and other information to share between scripts.
#########################################################################

export VARIABLES=./dcbikeshare_variables.sh
cat << EOF > ${VARIABLES}
demo_dbuser="${DbUserName}"
demo_db="${DatabaseName}"
demo_schema="${SchemaName}"
weather_dir="./weather-data"
get_metadata_script="/tmp/dcbikeshare_get_metadata.sh"
EOF

#########################################################################
# A simple test to check if we are running against a Greenplum database
# If so, make RANDOM distribution the default for new tables.
#########################################################################

psql -ec 'show gp_dbid' &> /dev/null
if [[ $? == 0  ]]; then
    gpconfig -c gp_create_table_random_default_distribution -v on
    gpstop -uq

    echo 'database="greenplum"' >> ${VARIABLES}
else
    echo 'database="postgres"' >> ${VARIABLES}
fi

#########################################################################
# Create a simple script to retrieve the DC Bike Share metadata and
# stream it to stdout. We will use Greenplum's external web table ability
# or PostgreSQL's file FDW to insert into a staging table.
#########################################################################

source ${VARIABLES}

cat << "EOF" > ${get_metadata_script}
#!/usr/bin/env bash
#
# Download the DC BikeShare system metadata
#

usage="$0 [ system_information | station_information | system_status | system_regions ]"
comment="Data is sent to stdout"

case "$1" in
 "system_information")   FILE=$1.json ;;
 "station_information")  FILE=$1.json ;;
 "station_status")       FILE=$1.json ;;
 "system_regions")       FILE=$1.json ;;
 *) echo "{ \"usage\": \"${usage}\" , \"comment\": \"${comment}\"}"; exit 1 ;;
esac

wget --quiet -O - https://gbfs.capitalbikeshare.com/gbfs/en/${FILE}
EOF

chmod +x ${get_metadata_script}

exists=$(psql -qAt -d $demo_db -c "select 1 from pg_tables where tablename = 'spatial_ref_sys'")
if [[ $exists != 1 ]]; then
    echo "** CAUTION ***** CAUTION ***** CAUTION ***** CAUTION ***** CAUTION ***** CAUTION *****"
    echo "**** PostGIS is not loaded in the database. Install the package before preceding. ****"
    echo "**************************************************************************************"
fi

which jq &> /dev/null
if [[ $? == 1 ]]; then
    cat << EOF
The JSON Query utility 'jq' is not in the PATH.
It is used in the loading of the weather data.
Install with your favorite package manager before proceeding.
(ie. CENTOS/RHEL: sudo yum -y install jq)
EOF
fi


cat << EOF

This demo makes use of a couple of PostgreSQL extensions and Greenplum's Web External Tables:
1. PostGIS: Used for both Greenplum and PostgreSQL backing databases.
2. File_FDW: Used for a PostgreSQL backing database.
   Superuser privs are required to install in the user's database.

       postgres# \c ${DatabaseName}
       postgres# create extension file_fdw;
       postgres# grant usage on foreign data wrapper file_fdw to ${DbUserName};

3. Web-based External Tables: Used for a Greenplum backing database.
   The demo user must have Superuser privs to create execute web external tables.

EOF
