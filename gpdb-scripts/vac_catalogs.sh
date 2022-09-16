#!/bin/bash

DBNAME="gpadmin"
VCOMMAND="VACUUM ANALYZE"

psql -tc "select '$VCOMMAND' || ' pg_catalog.' || relname || ';'
          from pg_class a,pg_namespace b
          where a.relnamespace=b.oid and b.nspname='pg_catalog' and a.relkind='r'" $DBNAME | psql -a $DBNAME
