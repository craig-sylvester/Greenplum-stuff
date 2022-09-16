DB=employee

echo '# Schema | Table | TableOID'
psql -t -A -d $DB << EOF
SELECT n.nspname as schema, c.relname as table, c.oid
FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE nspname in ( 'employees' )
  and not exists (select 1 from pg_partitions where partitiontablename = c.relname)
;
EOF
