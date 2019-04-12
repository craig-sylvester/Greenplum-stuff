/***********************************************************************
 * Return the last value in 5 minutes windows.
 ***************
 * First attempt at using a user defined function to truncate the
 * timestamp to the desired resolution in minutes.
 * You will also notice that we are using the first_value() function
 * instead of last_value() because our first attempt
 * with using last_value() + ascending sort did not return what
 * was expected. (This is illustrated later in our exploration.)
 *
 * Using first_value() and sorting the result in descending order
 * (which is functionally the same thing) returned our expected results.
 * The other thing to note here is that we receive duplicate rows
 * because the WINDOW function is applied to every row of the
 * partitioned result set.
 *
 * It is worth spending time to understand this.
 * Remember, the default frame for WINDOW functions is
 *    CURRENT ROW to UNBOUNDED PRECEDING
 * which works in this case.
 *
 * In the next SQL scripts, we will remove the duplicate rows.
 ***********************************************************************/

\i ../00_init.sql

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
limit 20
;
