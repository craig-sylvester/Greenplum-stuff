with jbo as ( select json_build_object(
                        'timeoutSeconds', 60),
                        'message', 'Idle session terminated',
                        'excludeIdleInTransaction', false),
                        'exemptedRoles', null)
)
insert into gpmetrics.gpcc_wlm_rule
(
    rule_id, rsgname, role, query_tag, dest_rsg, cpu_time,
    running_time, disk_io_mb, planner_cost, orca_cost,
    slice_num, action, active, ctime, etime, idle_session
)
SELECT
        1
    , 'admin_group'
    , ''
    , ''
    , ''
    , 0
    , 0
    , 0
    , 0
    , 0
    , 0
    , 4
    , true
    , now()
    , null
    , (select * from jbo)
WHERE NOT EXISTS (
    select
        rule_id
    from
        gpmetrics.gpcc_wlm_rule
    where rule_id = 1
    and  rsgname = %L
    and  idle_session::jsonb = (select * from jbo)::jsonb
    and etime is null
)
;
