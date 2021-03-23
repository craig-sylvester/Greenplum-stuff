/*****
 Returns the physical disk location(s) for a table
 If second argument is true, return locations for mirrors also

 Example usage:
    psql -d <dbname> -c "select schema, filepath from gp_relation_filepath('orders')"
    psql -d <dbname> -c "select schema, filepath from gp_relation_filepath('orders', true)"
*****/

drop function if exists public.gp_relation_filepath(text, boolean);
create or replace function public.gp_relation_filepath(in text, in boolean default false)
  returns table (host text, schema name, filepath text) as
$$
declare
    LOCAL_role_info text;
    LOCAL_query_txt text;

BEGIN
    if $2
    then
        LOCAL_role_info = ' 1=1 ';
    else
        LOCAL_role_info = ' s.role = ''p'' ';
    end if;

    LOCAL_query_txt = format('select s.hostname,
                                     n.nspname,
                                     s.datadir || ''/'' || a.fp
                              from
                                gp_dist_random(''pg_class'') c
                                    join pg_namespace n on c.relnamespace = n.oid and c.relname = %L
                                    join gp_segment_configuration s on s.content = c.gp_segment_id,
                                (select pg_relation_filepath(%L) fp) a
                              where %s',
                             $1, $1, LOCAL_role_info);

    return query execute LOCAL_query_txt;

END;
$$ LANGUAGE PLpgSQL;
----------------------------------------------------------------------------

/*****
 Returns the name of the table associated with this file node

 Example usage:
    psql -d <dbname> -c "select db, schema, table from gp_relation_table(82347)"
    where 82347 is in reference to file path "GP_SEG_DATADIR/base/<dboid>/82347"

 This function effectively replaces the PostgreSQL function:
    pg_filenode_relation (tblspace_id, filenode);
*****/

drop function if exists public.gp_relation_table(integer);
create function public.gp_relation_table(integer)
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
