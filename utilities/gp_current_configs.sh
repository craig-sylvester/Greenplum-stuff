#!/usr/bin/env bash

OUTPUT=appriss_gplum_settings.txt

source /usr/local/greenplum-db/greenplum_path.sh

psql -d postgres << EOF > ${OUTPUT}
select version();

\echo Current Resource Group Settings
select * from gp_toolkit.gp_resgroup_config ;

\echo Greenplum GUCs
show all;
EOF
