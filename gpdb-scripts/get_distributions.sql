-- Returns the distribution keys for all tables in the connected
-- databases. Does not list the child partitions for a partitioned
-- table.
-- Preserves dist key order - uses aggregates

SELECT 
  nspname as schema,
  relname as tbl,
  string_agg(attname, ', ' order by colorder) as dist_keys
FROM 
  (select localoid, unnest(attrnums) as colnum, generate_series(1, array_upper(attrnums, 1)) as colorder from gp_distribution_policy) d
  join pg_attribute a on (d.localoid = a.attrelid and d.colnum = a.attnum)
  join pg_class c on (d.localoid = c.oid)
  join pg_namespace n on (c.relnamespace = n.oid)
WHERE NOT EXISTS (select 1 from pg_partitions where relname = partitiontablename)
GROUP BY nspname, relname
;
