DROP EXTERNAL TABLE seg_env;
CREATE EXTERNAL WEB TABLE seg_env 
  (segment_id int, datadir text)
  execute 'echo -e $GP_SEGMENT_ID"\t"$GP_SEG_DATADIR' on all
  format 'text'
;

----------------------------------------------------------------------------

/*****
 Returns the physical disk location(s) for a table

 Example usage:
    psql -d <dbname> -c "select schema, filepath from pg_relation_table('orders')"
*****/

drop function if exists public.pg_relation_filepath(text);
create or replace function public.pg_relation_filepath(text)
  returns table (schema name, filepath text) as
$$
    select n.nspname,
           s.datadir || '/base/' || d.oid || '/' || c.relfilenode as filepath
    from
      gp_dist_random('pg_class') c
          join pg_namespace n on c.relnamespace = n.oid
                              and c.relname = $1
          join seg_env s on s.segment_id = c.gp_segment_id,
      pg_database d
    where
      d.datname = current_database()
    ;
$$ LANGUAGE SQL;

----------------------------------------------------------------------------

/*****
 Returns the name of the table associated with this file node

 Example usage:
    psql -d <dbname> -c "select db, schema, table from pg_relation_table(82347)"
    where 82347 is in reference to file path "GP_SEG_DATADIR/base/<dboid>/82347"
*****/

drop function if exists public.pg_relation_table(integer);
create function public.pg_relation_table(integer)
  returns table (db name, schema name, tbl name) as
$$
    select d.datname,
           n.nspname,
           c.relname
    from
      gp_dist_random('pg_class') c
          join pg_namespace n on c.relnamespace = n.oid
                              and c.relfilenode = $1,
      pg_database d
    where
      d.datname = current_database()
    group by 1,2,3
    ;
$$ LANGUAGE SQL;

----------------------------------------------------------------------------
