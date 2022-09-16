#!/usr/bin/env bash

#
#   Script to grant all permissions on a database schema objects to a user.
#   Grants SCHEMA, TABLE, and FUNCTION permissions.
#

# Verify number and validity of arguments
[[ $# != 3 ]] && { echo "Syntax: $0 <database> <schema> <user>"; exit 1; }
status=$(psql -At -d $1 -c "select 'valid' from pg_namespace n, pg_authid a where n.nspname = '$2' and a.rolname = '$3'")
[[ "x$status" != "xvalid" ]] && { echo "One or more of the arguments are invalid"; exit 2; }

DB=$1
SCHEMA=$2
USER=$3

LOGFILE="./perms_$1_$2_$3.log"

##############################################################################
# Set schema permissions. Possible values: USAGE, CREATE, ALL
SCHEMA_PERM='USAGE'

psql -t -q -d $DB -c "select 'grant ${SCHEMA_PERM} on schema ' || ('$SCHEMA') || ' to ' || ('$USER') || ';'" | psql -e -t -q -d $1 > $LOGFILE 2>&1
#############################################################################
# Set table permissions. Possible values: SELECT, INSERT, UPDATE, DELETE, ALL
# Other possible values are REFERENCES and TRIGGER but these are not used by GP
TBL_PERM='SELECT, INSERT, UPDATE, DELETE'

psql -t -q -d $DB -c "select 'grant ${TBL_PERMS} on ' || table_schema || '.' ||table_name || ' to ' || ('$USER') || ';'
                     from information_schema.tables
                     where table_catalog = ('$DB') and table_schema = ('$SCHEMA');" | psql -t -q -e -d $1 >> $LOGFILE 2>&1

##############################################################################
# Set function permissions. Possible values: EXECUTE, ALL
#FUNC_PERM='EXECUTE'

#psql -t -q -d $DB -c "select 'grant ${FUNC_PERM} on function ' || proname || '(' || pg_catalog.pg_get_function_arguments(p.oid) || ')' ||
#                     ' to ' || ('$USER') || ';'
#                     from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on p.pronamespace = n.oid
#                     where n.nspname = '$SCHEMA'" | psql -t -q -e -d $1 >> $LOGFILE 2>&1

##############################################################################
exit 0
