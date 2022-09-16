create or replace function grant_wildcard
(
  i_grant_cmd            varchar
)
returns int
as
$$
declare
  ln_cnt              int := 0;
  cmd_arr             varchar[];
  cmd_arr_new         varchar[];
  grant_type          varchar(10);
  grant_object        varchar;
  ls_sql              varchar;
  ls_grant_cmd        varchar;
  l_rec               record;
begin
  cmd_arr := regexp_split_to_array(lower(ltrim(rtrim(i_grant_cmd))), E'\\s+');
  if (cmd_arr[1] != 'grant' and cmd_arr[1] != 'revoke') then
    raise exception 'Command must start with grant or revoke';
  end if;
  grant_type := cmd_arr[2];
  raise notice '% : %', cmd_arr[1], grant_type;
  if (grant_type in ('select', 'insert', 'update', 'delete', 'all') and cmd_arr[4] != 'function') then
    grant_object := cmd_arr[4];
    raise notice '% : % : %', cmd_arr[1], grant_type, grant_object;
    ls_sql := 'select nspname || ''.'' || relname as objname
               from pg_class c join pg_namespace n on n.oid = c.relnamespace
               where nspname || ''.'' || relname like ''' || grant_object || '''
               and relkind = ''r''';
    for l_rec in execute ls_sql loop
      cmd_arr_new := cmd_arr;
      cmd_arr_new[4] := l_rec.objname;
      ls_grant_cmd := array_to_string(cmd_arr_new, ' ');
      raise notice '%', ls_grant_cmd;
      execute ls_grant_cmd;
      ln_cnt := ln_cnt + 1;
    end loop;
  end if;

  if (grant_type in ('execute', 'all')) then
    if (cmd_arr[4] = 'function') then
      grant_object := cmd_arr[5];
    else
      grant_object := cmd_arr[4];
    end if;
    ls_sql := 
    '
    SELECT n.nspname || ''.'' || p.proname as funcname,
           CASE WHEN proallargtypes IS NOT NULL THEN
             pg_catalog.array_to_string(ARRAY(
               SELECT
                 CASE
                   WHEN p.proargmodes[s.i] = ''i'' THEN ''''
                   WHEN p.proargmodes[s.i] = ''o'' THEN ''OUT ''
                   WHEN p.proargmodes[s.i] = ''b'' THEN ''INOUT ''
                   WHEN p.proargmodes[s.i] = ''v'' THEN ''VARIADIC ''
                 END ||
                 CASE
                   WHEN COALESCE(p.proargnames[s.i], '''') = '''' THEN ''''
                   ELSE p.proargnames[s.i] || '' '' 
                 END ||
                 pg_catalog.format_type(p.proallargtypes[s.i], NULL)
               FROM
                 pg_catalog.generate_series(1, pg_catalog.array_upper(p.proallargtypes, 1)) AS s(i)
             ), '', '')
           ELSE
             pg_catalog.array_to_string(ARRAY(
               SELECT
                 CASE
                   WHEN COALESCE(p.proargnames[s.i+1], '''') = '''' THEN ''''
                   ELSE p.proargnames[s.i+1] || '' ''
                   END ||
                 pg_catalog.format_type(p.proargtypes[s.i], NULL)
               FROM
                 pg_catalog.generate_series(0, pg_catalog.array_upper(p.proargtypes, 1)) AS s(i)
             ), '', '')
           END AS args
    FROM pg_catalog.pg_proc p
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
    ';
    ls_sql := 'select funcname || ''('' || coalesce(args,'''') || '')'' as objname from (' || ls_sql || ') a where a.funcname like ''' || grant_object || '''';
    for l_rec in execute ls_sql loop
      cmd_arr_new := cmd_arr;
      if (cmd_arr_new[4] = 'function') then
        cmd_arr_new[5] := l_rec.objname;
      else
        cmd_arr_new[4] := 'function ' || l_rec.objname;
      end if;
      ls_grant_cmd := array_to_string(cmd_arr_new, ' ');
      raise notice '%', ls_grant_cmd;
      execute ls_grant_cmd;
      ln_cnt := ln_cnt + 1;
    end loop;
  end if;
  return ln_cnt;
end
$$
language plpgsql volatile;
