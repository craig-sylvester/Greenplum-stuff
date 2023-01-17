/*****
 Install a Workload Mgmt rule to terminate idle sessions.

 Example usage:
    psql -d gpperfmon -c "select wlm_idle_terminate('default_group', 'hours', 12)"
*****/

drop function if exists public.wlm_idle_terminate (text, text, int, text, boolean, text);
create or replace function public.wlm_idle_terminate (
    IN res_group        text,
    IN timespan         text,
    IN ts_duration      integer,
    IN terminate_msg    text    default 'WLM: Idle session terminated',
    IN idleInTrans      boolean default false,
    IN exemptedRoles    text    default null
)
returns void as
$Body$
declare
    LOCAL_usage text = 'Usage: select wlm_idle_terminate(ResGrp, Timespan, Duration, [Msg], [IdleInTrans], [ExemptedRoles])';
    LOCAL_query_txt text;
    LOCAL_exception_msg text = '';
    LOCAL_int_val int = 0;
    LOCAL_boolean_val boolean = false;
    LOCAL_idle_session int = 4;

BEGIN
    /* Check for required input values */
    if res_group = '' then
        LOCAL_exception_msg = LOCAL_exception_msg || 'ResGrp ';
    end if;
    if timespan = '' then
        LOCAL_exception_msg = LOCAL_exception_msg || 'Timespan ';
    end if;
    if ts_duration  is null then
        LOCAL_exception_msg = LOCAL_exception_msg || 'Duration ';
    end if;

    if LOCAL_exception_msg != '' then
        RAISE EXCEPTION 'ERROR - missing: %', LOCAL_exception_msg
              USING HINT = LOCAL_usage;
    end if;

    /* Check for valid input values */
    LOCAL_query_txt = format('select 1 from gp_toolkit.gp_resgroup_config where groupname = %L', res_group);
    execute LOCAL_query_txt into LOCAL_int_val;
    if LOCAL_int_val is null then
        RAISE EXCEPTION 'ERROR - invalid resgroup: %', res_group
              USING HINT = LOCAL_usage;
    end if;

    /* The duration must be in the range of 30 seconds
       to 1 week (7 days * 24 hrs * 60 mins * 60 secs = 604800 seconds) */
    case timespan
       when 'hours' then
          if ts_duration      < 1 or ts_duration      > 168 then
              RAISE EXCEPTION 'ERROR - invalid ts_duration     : %', ts_duration     
                  USING HINT = '1 >=  Hours <= 168';
          end if;
          ts_duration      = ts_duration      * 60 * 60;
       when 'minutes' then
          if ts_duration      < 1 or ts_duration      > 10080 then
              RAISE EXCEPTION 'ERROR - invalid ts_duration     : %', ts_duration     
                  USING HINT = '1 >=  Minutes <= 10080 (1 week)';
          end if;
          ts_duration      = ts_duration      * 60;
       when 'seconds' then
          if ts_duration      < 30 or ts_duration      > 604800 then
              RAISE EXCEPTION 'ERROR - invalid ts_duration     : %', ts_duration     
                  USING HINT = '30 >=  Seconds <= 604800 (1 week)';
          end if;
       else
          RAISE EXCEPTION 'ERROR - invalid timespan: %', timespan
              USING HINT = 'Valid timespans are: hours, minutes, seconds';
    end case;
    timespan = 'timeoutSeconds';

    /* Check for an existing rule */
    LOCAL_query_txt = format($$
        select
            rule_id,
            rsgname,
            
        from
            gpmetrics.gpcc_wlm_rule
        where rsgname = %L
        and  json_typeof(idle_session->'timeoutSeconds') = 'number'
        and active = true
        and etime is null
        and action = LOCAL_idle_session
        $$,
        ts_duration     , terminate_msg, idleInTrans, exemptedRoles,
        res_group, res_group
    );
    LOCAL_query_txt = format($$
        with jbo as ( select json_build_object(
                             'timeoutSeconds', cast (%L as integer),
                             'message', %L,
                             'excludeIdleInTransaction', cast (%L as boolean),
                             'exemptedRoles', %L)
        )
        insert into gpmetrics.gpcc_wlm_rule
        (
            rule_id, rsgname, role, query_tag, dest_rsg, cpu_time,
            running_time, disk_io_mb, planner_cost, orca_cost,
            slice_num, action, active, ctime, etime, idle_session
        )
        SELECT
             1
           , %L
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
        $$,
        ts_duration     , terminate_msg, idleInTrans, exemptedRoles,
        res_group, res_group
    );
    execute LOCAL_query_txt;

    EXCEPTION
        WHEN OTHERS
        THEN RAISE NOTICE '%', SQLERRM;

    return;

END;
$Body$
LANGUAGE PLpgSQL
SECURITY definer;
