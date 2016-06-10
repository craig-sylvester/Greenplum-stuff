/***********************************************************************
 * Return the last value in 5 minutes windows.
 ***************
 * If you recall from step 05 (05_first_try.sql), we are getting the
 * results we expect but we have many duplicates.
 * Here we use the 05 SELECT as a subquery and use a GROUP BY in
 * the outer SELECT to remove the duplicates.
 ***********************************************************************/

\i ../00_init.sql

select measure_ts, lv
from (
    select 
       date_trunc_mins (measure_ts, 5) measure_ts,
       first_value(measure_value)
           over (partition by date_trunc_mins (measure_ts, 5)
                 order by measure_ts desc
                ) as lv
    from metrics
    where vehicle_id = 1
      and measure_ts between '2010-10-11 09:00:00'::timestamp
                         and '2010-10-11 16:00:00'::timestamp
) as a
group by measure_ts, lv
order by measure_ts
limit 10
;
