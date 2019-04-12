/***********************************************************************
 * Return the last value in X minutes windows.
 ***************
 * Here we use a PSQL variable for defining our desired resolution.
 * The "incr" variable is set when running the function from the
 * command line, i.e.:
 *     shell$ psql -v incr=10 -f q_xmin_incrs.sql
 ***********************************************************************/

\i ../00_init.sql

select measure_ts, lv
from (
    select 
       date_trunc_mins (measure_ts, :incr) measure_ts,
       first_value(measure_value)
           over (partition by date_trunc_mins (measure_ts, :incr)
                 order by measure_ts desc
                ) as lv
    from metrics
    where vehicle_id = 1
      and measure_ts between '2010-10-11 09:00:00'::timestamp
                         and '2010-10-11 16:00:00'::timestamp
) as a
group by measure_ts, lv
order by measure_ts
limit 15
;
