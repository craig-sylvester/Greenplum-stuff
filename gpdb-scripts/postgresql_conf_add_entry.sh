#!/bin/sh

##############################################################################
# To add a entry into the postgresql.conf
#
# Ideally you can use "gpconfig" to add the entry. However, if for any reason
# you are unable to execute the gpconfig or if gpconfig fails, then you can
# use the below script.
#
# Replace the <parameter-to-add=value> with parameter you want to add to
# the postgresql.conf
##############################################################################

PGOPTIONS='-c gp_session_role=utility' psql template1 -Atc " SELECT 'ssh '
        ||hostname 
        ||' \"echo <parameter-to-add=value> >> '|| f.fselocation 
        ||'/postgresql.conf\"' 
FROM pg_filespace_entry f , pg_tablespace t , gp_segment_configuration c
WHERE f.fsefsoid=t.spcfsoid 
  AND c.dbid=f.fsedbid
  AND t.oid=1663 " > /tmp/add_entry_to_postgresql_conf.sh
