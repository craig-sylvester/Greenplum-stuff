#!/usr/bin/env bash

source ./dcbikeshare_variables.sh

cat << EOF
A subset of the DC Bike Share data (monthly for 2018 & 2019 and Jan 2020) is available as GZIP compressed files
on Amazon's S3 service at
https://dc-bikeshare.s3.amazonaws.com/ (or s3://dc-bikeshare/)
NOTE: The data was unZIPped and re-compressed using GZIP. As of Feb 2020, PXF can access CSV files that have
      been compressed using gzip, bzip2, or snappy.

This script assumes that PXF has been installed and properly configured to access S3 file storage (Minio or AWS).
If this is not the case, refer to the documentation at:
https://gpdb.docs.pivotal.io/6-4/pxf/instcfg_pxf.html
EOF

# Let's make sure we are running against a Greenplum database.
mystatus=$(psql -d ${demo_db} -Atc "show gp_dbid" &> /dev/null)
[[ ${mystatus} == 1 ]] && { echo "Not a Greenplum database" ; exit 1;  }

TBL='trips'
EXT_TBL="ext_${TBL}"

# Verify the table exists before trying to load
mystatus=$(psql -d ${demo_db} -Atc "select 1 from information_schema.tables where table_schema = '${demo_schema}' and table_name = '${TBL}'")
[[ -z ${mystatus}  ]] && { echo "Table '${demo_schema}.${TBL}' does not exist." ; exit 1;  }

# Create the Greenplum external table
psql -e -d ${demo_db} << EOF
DROP EXTERNAL TABLE IF EXISTS ${demo_schema}.${EXT_TBL};
CREATE EXTERNAL TABLE ${demo_schema}.${EXT_TBL} (LIKE ${demo_schema}.${TBL})
   LOCATION ('pxf://dc-bikeshare/20*.csv.gz?PROFILE=s3:text&SERVER=s3&S3_SELECT=auto&FILE_HEADER=ignore&COMPRESSION_CODEC=gzip')
   FORMAT 'csv'
   LOG ERRORS
   SEGMENT REJECT LIMIT 10 PERCENT
;
EOF

[[ $? != 0  ]] && { echo "Problems creating external table. Exiting."; exit 1; }

psql -e -d ${demo_db} << EOF
\timing on
insert into ${demo_schema}.${TBL}  select * from ${demo_schema}.${EXT_TBL}
EOF
