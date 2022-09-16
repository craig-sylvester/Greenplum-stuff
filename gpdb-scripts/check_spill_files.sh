HOSTFILE=~/gpconfigs/hostfile
DBNAME=${1:-gpadmin}
BASEPATH=/data/gpdb/primary/

oid=$(psql -A -t -c "select oid from pg_database where datname = '${DBNAME}'")
echo Reporting for database \"$DBNAME\" with OID = $oid

gpssh -f ${HOSTFILE} -e "du -b ${BASEPATH}/gpseg*/base/$OID}/pgsql_tmp/*" | \
  grep -v "du" | sort | \
  awk -F" " '{ arr[$1] =  arr[$1] + $2 ; tot = tot + $2 };
             END { for ( i in arr )
                      print  "Segment node" i, arr[i], "bytes (" arr[i]/(1024**3)" GB)";
                      print "Total", tot, "bytes (" tot/(1024**3)" GB)" }'
