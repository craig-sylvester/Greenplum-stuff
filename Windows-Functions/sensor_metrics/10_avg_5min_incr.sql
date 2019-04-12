/***********************************************************************
 * Return the avg value for each 5 minute window.
 ***********************************************************************/

\i ../00_init.sql
\timing on

select measure_ts, avg_value::numeric(6,2) as "Avg_Value"
from (
    select
       date_trunc_mins (measure_ts, 5) measure_ts,
       AVG(measure_value)
           OVER ( PARTITION BY date_trunc_mins (measure_ts, 5) ) as avg_value
    from metrics
    where vehicle_id = 1
      and measure_ts between '2010-10-11 09:00:00'::timestamp
                         and '2010-10-11 16:00:00'::timestamp
) as a
group by measure_ts, avg_value
order by measure_ts
limit 10
;
