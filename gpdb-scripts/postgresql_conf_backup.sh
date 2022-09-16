#!/bin/sh

#########################################################################
# Creates a script to backup the postgresql.conf file on the master
# and all the segments.
# Example output file:
#
# ssh craighp "cp /data/gpdb/primary/gpseg1/postgresql.conf /data/gpdb/primary/gpseg1/postgresql.conf.backupcopy"
# ssh craighp "cp /data/gpdb/primary/gpseg0/postgresql.conf /data/gpdb/primary/gpseg0/postgresql.conf.backupcopy"
# ssh craighp "cp /data/gpdb/master/gpseg-1/postgresql.conf /data/gpdb/master/gpseg-1/postgresql.conf.backupcopy"
#########################################################################

PGOPTIONS='-c gp_session_role=utility' psql template1 -Atc " SELECT 'ssh '
       ||hostname 
       ||' \"cp '|| f.fselocation 
       ||'/postgresql.conf ' 
       || f.fselocation 
       || '/postgresql.conf.backupcopy\"' 
FROM pg_filespace_entry f , pg_tablespace t , gp_segment_configuration c
WHERE f.fsefsoid=t.spcfsoid 
  AND c.dbid=f.fsedbid
  AND t.oid=1663 " > /tmp/backup_postgresql_conf.sh
