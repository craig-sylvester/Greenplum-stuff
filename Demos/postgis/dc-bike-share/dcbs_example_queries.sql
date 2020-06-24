set search_path to dc_bikeshare, public;

select count(*) from dc_bikeshare.trips;

select date_trunc('hour', start_date) as "date",
       count(*) as "num trips",
       round(avg(duration)/60) as "avg duration (mins)",
       round(avg(st_distance(st_transform(s1.location, 3857),
                             st_transform(s2.location, 3857)))::NUMERIC / 1000.0, 2) as "avg distance (km)",
       round(avg(st_distance(st_transform(s1.location, 3857),
                             st_transform(s2.location, 3857)))::NUMERIC * 0.000621, 1) as "avg distance (miles)"
from dc_bikeshare.trips t
      join dc_bikeshare.station_info s1 on t.start_station_num::text = s1.short_name
      join dc_bikeshare.station_info s2 on t.end_station_num::text = s2.short_name
where start_date between '2018-06-01 00:00:00' and '2018-07-01 00:00:00'
group by 1 order by 1
;

select to_char(start_date, 'Day') as "Day", count(1) as "Num Trips"
from dc_bikeshare.trips t
group by 1 order by 2 desc;

/* For each day of the week (ie., "Sunday, Monday, etc"), return the number of trips and the percent of total trips taken for each weekday */

/***** old version
select to_char(start_date, 'Day') as "Day",
       count(1) as "Num Trips",
       round(count(1)::numeric / t.cnt::numeric * 100.0, 1) as "% of total"
from dc_bikeshare.trips tr
   cross join (select count(1) as cnt from dc_bikeshare.trips) t
group by 1, t.cnt
order by 2 desc
;
*****/

with data as (
select to_char(start_date, 'Day') as "day",
        count(1)
 from dc_bikeshare.trips
 group by 1
)

select day
  ,sum(count) over (partition by day) as dow_total
  ,round( sum(count) over (partition by day) / t.total * 100.0, 1) as "percent_of_total"
from data,
 (select count(*) total from dc_bikeshare.trips) t
order by 2 desc
;

/* Count the number of trips taken based on the air temperature */
select round(temp_fahrenheit) as "Temp F", count(*) as "Num Trips"
from dc_bikeshare.trips t
   left outer join dc_bikeshare.v_dc_weather v
        on date_trunc('hour', start_date) = date_trunc('hour', v.time_local)
where temp_fahrenheit is not null
group by 1
order by 2 desc;

/* Let's do a count of trips based on temperature ranges 
 * First we will use the view and go against the actual JSON data
 * Make note of the runtime ...
 */
select count(1) filter (where round(temp_fahrenheit) between -10 and 39) as "Below 40",
       count(1) filter (where round(temp_fahrenheit) between 40 and 49) as "40's",
       count(1) filter (where round(temp_fahrenheit) between 50 and 59) as "50's",
       count(1) filter (where round(temp_fahrenheit) between 60 and 69) as "60's",
       count(1) filter (where round(temp_fahrenheit) between 70 and 79) as "70's",
       count(1) filter (where round(temp_fahrenheit) between 80 and 89) as "80's",
       count(1) filter (where round(temp_fahrenheit) between 90 and 120) as "90+"
from dc_bikeshare.trips t
   left outer join dc_bikeshare.v_dc_weather w
     on date_trunc('hour', start_date) = date_trunc('hour', w.time_local)
where temp_fahrenheit is not null
;

/* Now let's do the same thing against a table we created from the view.
 * Again, make note of the runtime. In my testing, the runtime was reduced
 * by 4-5x when the view was stored as a database table.
 */
select count(1) filter (where round(temp_fahrenheit) between -10 and 39) as "Below 40",
       count(1) filter (where round(temp_fahrenheit) between 40 and 49) as "40's",
       count(1) filter (where round(temp_fahrenheit) between 50 and 59) as "50's",
       count(1) filter (where round(temp_fahrenheit) between 60 and 69) as "60's",
       count(1) filter (where round(temp_fahrenheit) between 70 and 79) as "70's",
       count(1) filter (where round(temp_fahrenheit) between 80 and 89) as "80's",
       count(1) filter (where round(temp_fahrenheit) between 90 and 120) as "90+"
from dc_bikeshare.trips t
   left outer join dc_bikeshare.dc_weather w
    on date_trunc('hour', start_date) = date_trunc('hour', w.time_local)
where temp_fahrenheit is not null
;


/*
 Let's take a look at the most popular bikes
 */

select bike_num, count(1) from dc_bikeshare.trips
group by bike_num
order by 2 desc
limit 5;

/*
 Now let's see the most popular bike by station
 */

select start_station_num, location, bike_num, cnt
from (select bike_num, start_station_num, count(1) cnt,
             rank() over (partition by start_station_num order by count(1) desc) as rnk
      from dc_bikeshare.trips
      group by bike_num, start_station_num) t
   join dc_bikeshare.station_info s on start_station_num::text = s.short_name 
where rnk = 1
order by 3 desc
limit 10;
