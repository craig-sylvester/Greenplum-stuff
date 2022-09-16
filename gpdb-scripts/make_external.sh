#
#   script to generate the external table definitions
#

# Verify number and validity of arguments
[[ $# != 2 ]] && { echo "Syntax:  $0 <database name> <schema name>"; exit 1; }
status=$(psql -At -d $1 -c "select 'valid' from pg_namespace n where n.nspname = '$2'")
[[ "x$status" != "xvalid" ]] && { echo "One or more of the arguments are invalid"; exit 2; }

DB=$1
SCHEMA=$2

# Get the version number of Postgres (80215 = GP 4.3 or earlier)
PG_80215=80215
pg_ver=$(psql -A -t -c 'show server_version_num')

# change these as needed 

EXTSCHEMA="ext"		# external table schema name
ERRSCHEMA="err"		# error table schema name
EXTENSION="txt"		# raw data file extension
GPFDIST_HOST="mdw"
GPFDIST_PORT=8082

echo -e "drop schema if exists $EXTSCHEMA cascade;\ncreate schema $EXTSCHEMA; "
if [[ $pg_ver == $PG_80215 ]]; then
    echo "
drop schema if exists $ERRSCHEMA cascade;
create schema $ERRSCHEMA;
grant all on schema $ERRSCHEMA to public;
"
fi
echo "grant all on schema $EXTSCHEMA to public;"

for t in $(psql $DB -t -A -c "select table_name from information_schema.tables where table_catalog = '$DB' and table_schema = '$SCHEMA' and table_type='BASE TABLE'  and is_insertable_into = 'YES' and not exists (select 1 from pg_partitions where partitiontablename = table_name);")
do
    echo -e "\n-- EXTERNAL TABLE for $SCHEMA.$t"
    echo "create external table $EXTSCHEMA.$t (like $SCHEMA.$t)"
# change if pxf 
    echo "location ('gpfdist://$GPFDIST_HOST:$GPFDIST_PORT/$SCHEMA.$t.$EXTENSION')"
# change format information as needed
    echo "format 'text' (delimiter as '|' null as '' newline as 'CRLF')"
    if [[ $pg_ver == $PG_80215 ]] ; then
        echo "log errors into err.$t segment reject limit 100 rows;"
    else
        echo "log errors segment reject limit 100 rows;"
    fi
    echo ""
done

exit 0
