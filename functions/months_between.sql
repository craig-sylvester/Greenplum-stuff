create or replace function months_between (date, date)
returns int as
$$
  select (date_part ('year', f) * 12 + date_part ('month', f) )::int
  from age($1, $2) f;
$$
language SQL
immutable strict
;
