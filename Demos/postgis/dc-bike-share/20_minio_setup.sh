#!/usr/bin/env bash

source ./dcbikeshare_variables.sh

[[ ${database} == "postgres" ]] && { echo "PXF+minio not used for PostgreSQL"; exit 0; }

cat << EOF

Minio provides S3 protocol access to data stored 'locally'. More info here:
https://min.io/
As a quick demo, we will set up a Minio server, download the current Station
Information, and use PXF's support for reading JSON files to select the data.

This script assumes you have installed Minio and you have installed and initialized PXF.
If you are running Greenplum on a cluster created using VMware Tanzu's Greenplum marketplace offering,
PXF is already installed and you just need to configure the server(s) you want to access.
If not, refer to the docs below to install PXF (steps 1,2, and 6):
https://gpdb.docs.pivotal.io/6-7/pxf/instcfg_pxf.html

EOF
read -p 'Hit ENTER to proceed or CNTL-C to exit'

set -u

MINIO_DIR="/tmp/minio_demo"
MINIO_BUCKET="${MINIO_DIR}/data"

# Create a Minio server directory and a bucket
mkdir -p ${MINIO_DIR} ${MINIO_BUCKET}

echo Start minio server
minio server ${MINIO_DIR} &> ${MINIO_DIR}/server.log &
echo $! > ${MINIO_DIR}/server.pid

sleep 2

#S3_KEY=$(jq -r '.credential.accessKey' ${MINIO_DIR}/.minio.sys/config/config.json)
#S3_SECRET_KEY=$(jq -r '.credential.secretKey' ${MINIO_DIR}/.minio.sys/config/config.json)
S3_KEY=$(grep '"accessKey"' ${MINIO_DIR}/.minio.sys/config/config.json | cut -d'"' -f4)
S3_SECRET_KEY=$(grep '"secretKey"' ${MINIO_DIR}/.minio.sys/config/config.json | cut -d'"' -f4)

# Retrieve the station information data from the dc bikeshare website
${get_metadata_script} station_information > ${MINIO_BUCKET}/station_information.json

#### PXF Setup ####

PXF_ServerDir=${PXF_CONF}/servers/minio
PXF_SiteFile=${PXF_ServerDir}/minio-site.xml

mkdir -p ${PXF_ServerDir}

# Create an S3 Minio config file
echo "Creating '${PXF_SiteFile}' ..."
cat << EOF > ${PXF_SiteFile}
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://mdw:9000/</value>
    </property>
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
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
</configuration>
EOF

echo pxf cluster sync
pxf cluster sync

echo pxf cluster restart
pxf cluster restart
