/***********************************************************************
 * Return the last value in 5 minutes windows.
 ***************
 * Our first attempt at using the last_value() function.
 *
 * By default, a frame is "CURRENT ROW to UNBOUNDED PRECEDING"
 * So, in this attempt, we get multiple values for the Last Value
 * since the Last Value is always the current row.
 * Note that we also removed DESC from the order by in the over()
 * clause.
 ***********************************************************************/

\i ../00_init.sql

select measure_ts, lv
from (
    select 
       date_trunc_mins (measure_ts, 5) measure_ts,
       last_value(measure_value)
           over (partition by date_trunc_mins (measure_ts, 5)
                 order by measure_ts
                ) as lv
    from metrics
    where vehicle_id = 1
      and measure_ts between '2010-10-11 09:00:00'::timestamp
                         and '2010-10-11 16:00:00'::timestamp
) as a
group by measure_ts, lv
order by measure_ts
limit 20
;
