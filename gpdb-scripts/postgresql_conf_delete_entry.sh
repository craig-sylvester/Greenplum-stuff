#!/bin/sh

##############################################################################
# To remove a entry from the postgresql.conf
#
# Replace the <parameter-to-remove> with parameter (or something that you can
# uniquely identify the parameter) you wish to remove from postgresql.conf.
##############################################################################

PGOPTIONS='-c gp_session_role=utility' psql template1 -Atc " SELECT 'ssh '
       ||hostname 
       ||' \"grep -v <parameter-to-remove> '|| f.fselocation 
       ||'/postgresql.conf.backupcopy > '
       || f.fselocation 
       || '/postgresql.conf\"' 
FROM pg_filespace_entry f , pg_tablespace t , gp_segment_configuration c
WHERE f.fsefsoid=t.spcfsoid 
  AND c.dbid=f.fsedbid
  AND t.oid=1663 " > /tmp/remove_entry_from_postgresql_conf.sh
