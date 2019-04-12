\i ../00_init.sql
\set tblname metrics

drop table if exists :tblname;
create table :tblname
(
    vehicle_id    int,
    measure_id    int,
    measure_ts    timestamp,
    measure_value float8
)
distributed randomly
;
