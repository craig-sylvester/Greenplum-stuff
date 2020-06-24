#!/usr/bin/env bash

source ./dcbikeshare_variables.sh

[[ ${database} == "postgres" ]] && { echo "PXF not used for PostgreSQL"; exit 0; }
[[ ${USER} != "gpadmin" ]] && { echo "This script must be run by gpadmin"; exit 0; }

cat << EOF

The DC Bike Share 'trips' data is stored on AWS S3 in ZIP compressed CSV files.
Greenplum's gpfdist and PXF utilities currently do not support ZIP compressed files.
For the purposes of this demo, I have converted the ZIP compressed files to
GZIP compressed files and placed the data in a publically accessible S3 bucket.

This script assumes you have already installed and initialized PXF. If you are running
Greenplum on a cluster created using VMware's marketplace offerings, you can easily
install PXF by running 'gpoptional' as administrator (gpadmin).
If not, refer to the docs below to install PXF (steps 1,2, and 6):
https://gpdb.docs.pivotal.io/6-7/pxf/instcfg_pxf.html

EOF
read -p 'Hit ENTER to proceed or CNTL-C to exit'

set -eu

read -p "Enter in AWS S3 key: " answer
[[ ! -z ${answer} ]] && S3_KEY=${answer} || exit 1

read -p "Enter in AWS S3 secret key: " answer
[[ ! -z ${answer} ]] && S3_SECRET_KEY=${answer} || exit 1

S3_ServerDir=${PXF_CONF}/servers/s3
S3_SiteFile=${S3_ServerDir}/s3-site.xml

# Create a PXF server for S3
mkdir -p ${S3_ServerDir}

# Create an S3 site file
echo "Creating '${S3_SiteFile}' ..."
cat << EOF > ${S3_SiteFile}
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.s3a.access.key</name>
        <value>${S3_KEY}</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>${S3_SECRET_KEY}</value>
    </property>
    <property>
        <name>fs.s3a.fast.upload</name>
        <value>true</value>
    </property>
</configuration>
EOF

echo pxf cluster sync
pxf cluster sync

echo pxf cluster start
pxf cluster start
