select s.inst_id,s.sid
,      CASE WHEN state != 'WAITING' THEN 'WORKING' ELSE 'WAITING' END AS state
,      CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue' ELSE event END AS sw_event
,      CASE WHEN state = 'WAITING' AND event like 'enq%' THEN mod(p1,16) END AS lock_mode
,      count(*) over (partition by CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue' ELSE event END) evnt_cnt 
,      count(*) over (partition by sql_id) sql_cnt 
,      s.seconds_in_wait,s.blocking_session,s.final_blocking_session,s.sql_id,s.module,s.sql_exec_id,to_char(s.sql_exec_start,'DD-MON-YYYY HH24:MI:SS') exec_start
, round((sysdate - s.sql_exec_start)*24*60*60) duration
,      (select px_servers_allocated||'/'||px_servers_requested from v$sql_monitor m where m.sql_id = s.sql_id and s.sql_exec_id = m.sql_exec_id and m.sql_exec_Start = s.sql_exec_Start and process_name not like 'p%') px_details
,      (select sql_fulltext from v$sql t where t.sql_id = s.sql_id and rownum = 1) txt
,      s.*
from   gv$session      s
where  s.status    = 'ACTIVE'
and    s.username IS NOT NULL
and    s.sid != (select sid from v$mystat where rownum = 1)
AND    s.event NOT IN 
       ('Null event','client message','KXFX: Execution Message Dequeue - Slave','PX Deq: Execution Msg','KXFQ: kxfqdeq - normal dequeue','PX Deq: Table Q Normal',
        'Wait for credit - send blocked','PX Deq Credit: send blkd','Wait for credit - need buffer to send','PX Deq Credit: need buffer','Wait for credit - free buffer',
        'PX Deq Credit: free buffer','parallel query dequeue wait','PX Deque wait','Parallel Query Idle Wait - Slaves','PX Idle Wait','slave wait','dispatcher timer',
        'virtual circuit status','pipe get','rdbms ipc message','rdbms ipc reply','pmon timer','smon timer','PL/SQL lock timer','SQL*Net message from client','WMON goes to sleep',
        'Streams AQ: waiting for messages in the queue','class slave wait')
        order by 1,3 DESC, 4;

alter session set nls_date_format = 'DD-MON-YYYY HH24:MI:SS';

select *
from   v$sysmetric_summary
where  metric_name     = 'Average Synchronous Single-Block Read Latency';

select count(*) over (), s.* from dba_subscr_registrations s where subscription_name = '"SYS"."SRVQUEUE":"TAFTSM"'; 

select snap_id, begin_time, end_time, round(average,2)
from dba_hist_sysmetric_summary
where  to_char(end_time,'D') not in (6,7)
and   metric_name     = 'Average Synchronous Single-Block Read Latency'
and   instance_number =2
--and average > 15
order by snap_id desc;
		
select s.machine
,      lo.inst_id  
,      lo.object_id  
,      lo.session_id  
,      lo.os_user_name  
,      lo.process  
,      lo.locked_mode  
,      ob.owner  
,      ob.object_name  
,      ob.subobject_name
,      tx.addr  
,      tx.start_time txn_start_time  
,      tx.status  
,      tx.xid
,      s.*  
from   gv$locked_object lo  
,      dba_objects      ob  
,      gv$transaction    tx  
,      gv$session        s  
where  ob.object_id = lo.object_id  
and    tx.xidusn    (+) = lo.xidusn  
and    tx.xidslot   (+) = lo.xidslot  
and    tx.xidsqn    (+) = lo.xidsqn  
and    s.taddr      (+) = tx.addr
order by txn_start_time, session_id, object_name;
        
select s.machine
,      tx.start_time txn_start_time 
,      tx.xid
,      s.sid
,      CASE WHEN state != 'WAITING' THEN 'WORKING' ELSE 'WAITING' END AS state
,      CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue' ELSE event END AS sw_event
,      s.seconds_in_wait, s.sql_exec_start, s.prev_exec_start
,      tx.addr  
,      tx.status  
,      s.*  
from   gv$transaction    tx  
,      gv$session        s  
where  s.taddr      (+) = tx.addr
order by txn_start_time, s.sid;

select count(*) over (partition by h.sample_time) sess_cnt
,      h.user_id
,      (select username from dba_users u where u.user_id = h.user_id) u, h.service_hash
,      xid, sample_id, sample_time, session_state, session_id, session_serial#,sql_id, sql_exec_id, sql_exec_start, event, p1, mod(p1,16), blocking_session,blocking_session_serial#, current_obj#
,      (select object_name||' - '||subobject_name from dba_objects where object_id = current_obj#) obj
--,      (select sql_fulltext from v$sql s where s.sql_id = h.sql_id and rownum = 1) sqltxt
,      (select sql_text from dba_hist_sqltext s where s.sql_id = h.sql_id and rownum = 1) sqltxt
, h.*
from   v$active_session_history h
order by h.sample_id desc;
     
select count(*) over (partition by h.sample_time, h.instance_number) sess_cnt
--,      h.user_id
,      (select username from dba_users u where u.user_id = h.user_id) u
--, h.service_hash,      xid, sample_id
, to_char(sample_time,'DD-MON HH24:MI:SS') st, session_state, session_id, session_serial#,sql_id, sql_exec_id, sql_exec_start, event
--, p1, mod(p1,16)
, blocking_session,blocking_session_serial#
,      (select object_name from dba_procedures p where p.object_id = h.plsql_entry_object_id and rownum = 1)||'.'||
       (select procedure_name from dba_procedures p where p.object_id = h.plsql_entry_object_id and p.subprogram_id = h.plsql_entry_subprogram_id) prog1
,      (select object_name from dba_procedures p where p.object_id = h.plsql_object_id and rownum = 1)||'.'||
       (select procedure_name from dba_procedures p where p.object_id = h.plsql_object_id and p.subprogram_id = h.plsql_subprogram_id) prog2
--, current_obj#
,      (select object_name||' - '||subobject_name from dba_objects where object_id = current_obj#) obj
--,      (select sql_fulltext from v$sql s where s.sql_id = h.sql_id and rownum = 1) sqltxt
,      (select sql_text from dba_hist_sqltext s where s.sql_id = h.sql_id and rownum = 1) sqltxt
, h.*
from   dba_hist_active_sess_history h
,      dba_hist_snapshot            s
where  h.dbid            = s.dbid
and    h.snap_id         = s.snap_id
and    h.instance_number = s.instance_number
and    s.end_interval_time between to_date('08/09/2015 10:55','DD/MM/YYYY HH24:MI') and to_date('08/09/2015 14:05','DD/MM/YYYY HH24:MI')
and    h.sample_time       between to_date('08/09/2015 11:00','DD/MM/YYYY HH24:MI') and to_date('08/09/2015 13:20','DD/MM/YYYY HH24:MI')
order by h.sample_id;

select sql_id, sql_exec_id, sql_exec_start, count(*), min(sample_time), max(sample_time)
from   dba_hist_active_sess_history h
,      dba_hist_snapshot            s
where  h.dbid            = s.dbid
and    h.snap_id         = s.snap_id
and    h.instance_number = s.instance_number
--and   h.instance_number = 2
--and    h.user_id = 342
and    h.sql_id = 'ga1k0tn5ncx9c'
and    s.end_interval_time between to_date('29/09/2015 17:00','DD/MM/YYYY HH24:MI') and to_date('29/09/2015 20:00','DD/MM/YYYY HH24:MI')
and    h.sample_time between to_date('29/09/2015 17:00','DD/MM/YYYY HH24:MI') and to_date('29/09/2015 20:00','DD/MM/YYYY HH24:MI')
group by sql_id, sql_exec_id, sql_exec_start
order by count(*) desc;

select trunc(sql_exec_start,'HH24') period, sql_id, sql_exec_id, sql_exec_start, count(*), min(sample_time), max(sample_time)
from   dba_hist_active_sess_history h
where  h.sql_id = 'cpuwsrbz9m96a'
group by trunc(sql_exec_start,'HH24'),sql_id, sql_exec_id, sql_exec_start
order by trunc(sql_exec_start,'HH24') desc, count(*) desc;

select sql_plan_line_id, sql_plan_operation, current_obj#, count(*), min(sample_time), max(sample_time)
,      (select object_name||' - '||subobject_name from dba_objects where object_id = current_obj#) obj
from   dba_hist_active_sess_history h
,      dba_hist_snapshot            s
where  h.dbid            = s.dbid
and    h.snap_id         = s.snap_id
and    h.instance_number = s.instance_number
and    s.end_interval_time between to_date('15/12/2015 00:00','DD/MM/YYYY HH24:MI') and to_date('16/12/2015 00:00','DD/MM/YYYY HH24:MI')
and    h.sample_time       between to_date('15/12/2015 00:00','DD/MM/YYYY HH24:MI') and to_date('16/12/2015 00:00','DD/MM/YYYY HH24:MI')
and    h.sql_id = 'cpuwsrbz9m96a' and h.sql_exec_id = 33554433
and    h.instance_number = 2
group by sql_plan_line_id, sql_plan_operation, current_obj#
order by sql_plan_line_id ;
        
select sn.snap_id
,      to_char(sn.end_interval_time,'DD-MON-YYYY HH24:MI') dt
,      st.instance_number inst
,      st.sql_id
,      st.plan_hash_value
,      st.fetches_delta     fchs
,      rows_processed_delta rws
,      executions_delta     execs
,      elapsed_time_delta/1000/1000   elp
,      round(elapsed_time_delta/1000/1000/nvl(nullif(executions_delta,0),1),2)   elpe
,      cpu_time_delta/1000/1000       cpu
,      buffer_gets_delta    gets
,      iowait_delta/1000/1000         io
,      clwait_delta/1000/1000         cl
,      ccwait_delta/1000/1000         cc
,      apwait_delta/1000/1000         ap
,      plsexec_time_total/1000/1000   pl
,      round(disk_reads_delta)         disk_reads
,      round(direct_writes_delta)       direct_writes
from   dba_hist_snapshot sn
,      dba_hist_sqlstat  st
where  st.snap_id            = sn.snap_id
and    sn.instance_number = st.instance_number
and    st.sql_id             IN ('7vc631m2ayz8k')
order by sn.snap_id desc; 

select trunc(sn.end_interval_time) dt
,      st.sql_id
,      sum(st.fetches_delta) fch
,      sum(rows_processed_delta) rws
,      sum(executions_delta)     execs
,      round(sum(elapsed_time_delta)/1000/1000)   elp
,      round(sum(elapsed_time_delta)/1000/1000/nvl(nullif(sum(executions_delta),0),1),2)   elpe
,      round(sum(cpu_time_delta)/1000/1000)       cpu
,      sum(buffer_gets_delta)    gets
,      round(sum(iowait_delta)/1000/1000)         io
,      round(sum(clwait_delta)/1000/1000)         cl
,      round(sum(ccwait_delta)/1000/1000)         cc
,      round(sum(apwait_delta)/1000/1000)         ap
,      round(sum(plsexec_time_total)/1000/1000)   pl
,      round(sum(disk_reads_delta))         disk_reads
,      round(sum(direct_writes_delta))        direct_writes
from   dba_hist_snapshot sn
,      dba_hist_sqlstat  st
where  st.snap_id            = sn.snap_id
and    sn.instance_number = st.instance_number
and    st.sql_id             IN ('drsgryg735xqu')
group by trunc(sn.end_interval_time), st.sql_id
order by trunc(sn.end_interval_time) desc, elp+cpu desc; 


select x.*, (select sql_text from dba_hist_sqltext t where t.sql_id = x.sql_id and rownum = 1) txt
from (
select sn.snap_id
,      to_char(sn.end_interval_time,'DD-MON-YYYY HH24:MI') dt
,      st.sql_id
--, st.sql_profile
--,      st.instance_number inst
--,      st.parsing_schema_name psn
,      st.plan_hash_value phv
,      sum(st.fetches_delta) fch
,      sum(rows_processed_delta) rws
,      sum(executions_delta)     execs
,      round(sum(elapsed_time_delta)/1000/1000)   elp
,      round(sum(elapsed_time_delta)/1000/1000/nvl(nullif(sum(executions_delta),0),1),2)   elpe
,      round(sum(cpu_time_delta)/1000/1000)       cpu
,      sum(buffer_gets_delta)    gets
,      round(sum(iowait_delta)/1000/1000)         io
--,      round(sum(clwait_delta)/1000/1000)         cl
--,      round(sum(ccwait_delta)/1000/1000)         cc
--,      round(sum(apwait_delta)/1000/1000)         ap
--,      round(sum(plsexec_time_delta)/1000/1000)   pl
,      round(sum(disk_reads_delta))         disk_reads
--,      round(sum(direct_writes_delta))        direct_writes
,      row_number() over (partition by sn.snap_id, st.instance_number
                          order by sum(elapsed_time_delta) desc) rn
from   dba_hist_snapshot sn
,      dba_hist_sqlstat  st
where  st.snap_id            = sn.snap_id
and    sn.instance_number = st.instance_number
and    sn.instance_number = 2
and    to_char(sn.end_interval_time,'D') not in (6,7)
--and    to_char(sn.end_interval_time,'HH24') >= 19
--and    st.sql_id = '7vc631m2ayz8k'
group by 
       sn.snap_id
,      sn.end_interval_time
,      st.sql_id, st.sql_profile
,      st.instance_number
,      st.parsing_schema_name
,      st.plan_hash_value
) x
where rn <= 5
order by snap_id desc, rn;

select sn.snap_id, to_char(sn.begin_interval_time,'DD-MON-YYYY HH24:MI') begin_interval_time
,      round(max(case when sn.instance_number = 1 then average end)) avg_cpu_inst_1
,      round(max(case when sn.instance_number = 2 then average end)) avg_cpu_inst_2
,      round(max(case when sn.instance_number = 1 then maxval end)) peak_cpu_inst_1
,      round(max(case when sn.instance_number = 2 then maxval end)) peak_cpu_inst_2
from   dba_hist_sysmetric_summary mt
,      dba_hist_snapshot          sn
where  mt.snap_id         = sn.snap_id
and    mt.instance_number = sn.instance_number
and    mt.metric_name     = 'Host CPU Utilization (%)'
and    sn.begin_interval_time > trunc(sysdate)-30
and    TO_CHAR(begin_interval_time,'D') not in (6,7)
group by sn.snap_id, to_char(sn.begin_interval_time,'DD-MON-YYYY HH24:MI')
order by sn.snap_id desc;

WITH subq_snaps AS
(SELECT dbid                dbid
 ,      instance_number     inst
 ,      snap_id             e_snap
 ,      lag(snap_id) over (partition by instance_number, startup_time order by snap_id) b_snap
 ,      TO_CHAR(begin_interval_time,'D') b_day
 ,      TO_CHAR(begin_interval_time,'DD-MON-YYYY HH24:MI') b_time
 ,      TO_CHAR(end_interval_time,'HH24:MI')   e_time
 ,    ((extract(day    from (end_interval_time - begin_interval_time))*86400)
     + (extract(hour   from (end_interval_time - begin_interval_time))*3600)
     + (extract(minute from (end_interval_time - begin_interval_time))*60)
     + (extract(second from (end_interval_time - begin_interval_time)))) duration
 FROM   dba_hist_snapshot)
SELECT ss.inst
,      ss.b_snap
,      ss.e_snap
,      ss.b_time
,      ss.e_time
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'DB time' THEN em.value - bm.value END)/1000000/60,2),'999999990.99')                                  db_time
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'DB time' THEN em.value - bm.value END)/(ss.duration*1000000),1),'999999990.99')        aas
,      (SELECT round(average,2)
        FROM   dba_hist_sysmetric_summary sm
        WHERE  sm.dbid            = ss.dbid
        AND    sm.snap_id         = ss.e_snap
        AND    sm.instance_number = ss.inst
        AND    sm.metric_name     = 'Average Synchronous Single-Block Read Latency'
        AND    sm.group_id        = 2)                                                                                                                   assbl     
,      (SELECT round(average,2)
        FROM   dba_hist_sysmetric_summary sm
        WHERE  sm.dbid            = ss.dbid
        AND    sm.snap_id         = ss.e_snap
        AND    sm.instance_number = ss.inst
        AND    sm.metric_name     = 'Host CPU Utilization (%)'
        AND    sm.group_id        = 2)                                                                                                                   cpu_util
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'DB CPU' THEN em.value - bm.value END)/1000000,2),'999999990.99')                                      db_cpu
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'sql execute elapsed time' THEN em.value - bm.value END)/1000000,2),'999999990.99')                    sql_time
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'PL/SQL execution elapsed time' THEN em.value - bm.value END)/1000000,2),'999999990.99')               plsql_time
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'parse time elapsed' THEN em.value - bm.value END)/1000000,2),'999999990.00')                          parse_time
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'failed parse elapsed time' THEN em.value - bm.value END)/1000000,2),'999999990.99')                   failed_parse
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'hard parse (sharing criteria) elapsed time' THEN em.value - bm.value END)/1000000,2),'999999990.99')  hard_parse_sharing
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'RMAN cpu time (backup/restore)' THEN em.value - bm.value END)/1000000,2),'999999990.99')              rman_cpu
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'connection management call elapsed time' THEN em.value - bm.value END)/1000000,2),'999999990.99')     connection_mgmt
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'sequence load elapsed time' THEN em.value - bm.value END)/1000000,2),'999999990.99')                  sequence_load
,      TO_CHAR(ROUND(100*MAX(CASE WHEN bm.stat_name = 'DB CPU' THEN em.value - bm.value END) 
           / NULLIF(MAX(CASE WHEN bm.stat_name = 'DB time' THEN em.value - bm.value END),0),2),'999999990.99')                                           db_cpu_perc
,      TO_CHAR(ROUND(100*MAX(CASE WHEN bm.stat_name = 'sql execute elapsed time' THEN em.value - bm.value END)
           / NULLIF(MAX(CASE WHEN bm.stat_name = 'DB time' THEN em.value - bm.value END),0),2),'999999990.99')                                           sql_time_perc
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'PL/SQL execution elapsed time' THEN em.value - bm.value END)
           / NULLIF(MAX(CASE WHEN bm.stat_name = 'DB time' THEN em.value - bm.value END),0),2),'999999990.99')                                           plsql_time_perc
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'parse time elapsed' THEN em.value - bm.value END)
           / NULLIF(MAX(CASE WHEN bm.stat_name = 'DB time' THEN em.value - bm.value END),0),2),'999999990.99')                                           parse_time_perc
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'failed parse elapsed time' THEN em.value - bm.value END)
           / NULLIF(MAX(CASE WHEN bm.stat_name = 'DB time' THEN em.value - bm.value END),0),2),'999999990.99')                                           failed_parse_perc
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'hard parse (sharing criteria) elapsed time' THEN em.value - bm.value END)
           / NULLIF(MAX(CASE WHEN bm.stat_name = 'DB time' THEN em.value - bm.value END),0),2),'999999990.99')                                           hard_parse_sharing_perc
,      TO_CHAR(ROUND(MAX(CASE WHEN bm.stat_name = 'RMAN cpu time (backup/restore)' THEN em.value - bm.value END)
           / NULLIF(MAX(CASE WHEN bm.stat_name = 'DB time' THEN em.value - bm.value END),0),2),'999999990.99')                                           rman_cpu_perc
FROM  subq_snaps              ss
,     dba_hist_sys_time_model em  
,     dba_hist_sys_time_model bm  
WHERE bm.dbid                   = ss.dbid
AND   bm.snap_id                = ss.b_snap  
AND   bm.instance_number        = ss.inst
AND   em.dbid                   = ss.dbid
AND   em.snap_id                = ss.e_snap
AND   em.instance_number        = ss.inst
AND   bm.stat_id                = em.stat_id  
GROUP BY 
       ss.dbid
,      ss.inst
,      ss.b_day
,      ss.b_snap
,      ss.e_snap
,      ss.b_time
,      ss.e_time
,      ss.duration
HAVING b_day NOT IN (6,7)
AND    inst = 2 
--AND b_snap = 18673
--AND    e_time = '17:00'
ORDER BY b_snap DESC;

WITH subq_snaps AS
(SELECT dbid                dbid
 ,      instance_number     inst
 ,      snap_id             e_snap
 ,      lag(snap_id) over (partition by instance_number, startup_time order by snap_id) b_snap
 ,      TO_CHAR(begin_interval_time,'D') b_day
 ,      TO_CHAR(begin_interval_time,'DD-MON-YYYY HH24:MI') b_time
 ,      TO_CHAR(end_interval_time,'HH24:MI')   e_time
 ,    ((extract(day    from (end_interval_time - begin_interval_time))*86400)
     + (extract(hour   from (end_interval_time - begin_interval_time))*3600)
     + (extract(minute from (end_interval_time - begin_interval_time))*60)
     + (extract(second from (end_interval_time - begin_interval_time)))) duration
 FROM   dba_hist_snapshot)
,    io_stats AS
(SELECT ss.*
 ,      bv.event_name
 ,      ev.time_waited_micro_fg - bv.time_waited_micro_fg time_waited_micro
 ,      ev.total_waits_fg       - bv.total_waits_fg       waits
 FROM   subq_snaps            ss
 ,      dba_hist_system_event bv
 ,      dba_hist_system_event ev
 WHERE  bv.dbid                   = ss.dbid
 AND    bv.snap_id                = ss.b_snap  
 AND    bv.instance_number        = ss.inst
 AND    bv.event_name            IN ('db file sequential read','direct path read','direct path read temp','db file scattered read','db file parallel read')
 AND    ev.dbid                   = ss.dbid
 AND    ev.snap_id                = ss.e_snap
 AND    ev.instance_number        = ss.inst
 AND    ev.event_id               = bv.event_id)
SELECT io.dbid
,      io.inst
,      io.b_snap
,      io.e_snap
,      io.b_time
,      io.e_time
,      (SELECT ROUND(average,2)
        FROM   dba_hist_sysmetric_summary sm
        WHERE  sm.dbid            = io.dbid
        AND    sm.snap_id         = io.e_snap
        AND    sm.instance_number = io.inst
        AND    sm.metric_name     = 'Average Synchronous Single-Block Read Latency'
        AND    sm.group_id        = 2) assbl
,      MAX(CASE WHEN event_name = 'db file sequential read' THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END) END) single_waits
,      MAX(CASE WHEN event_name = 'db file scattered read'  THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END) END) multi_waits
,      MAX(CASE WHEN event_name = 'db file parallel read'   THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END) END) prefch_wait
,      MAX(CASE WHEN event_name = 'direct path read'        THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits  END) END) direct_waits
,      MAX(CASE WHEN event_name = 'direct path read temp'   THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END) END)  temp_waits
,      MAX(CASE WHEN event_name = 'db file sequential read' THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END/duration) END) iops_single
,      MAX(CASE WHEN event_name = 'db file sequential read' THEN ROUND(CASE WHEN time_waited_micro/1000/1000 < 0 THEN NULL ELSE time_waited_micro/1000/1000 END) END) single_secs_total
,      MAX(CASE WHEN event_name = 'db file sequential read' THEN ROUND((time_waited_micro/1000)/NULLif(waits,0)) END) single_avg
,      MAX(CASE WHEN event_name = 'db file scattered read'  THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END/duration) END) iops_multi
,      MAX(CASE WHEN event_name = 'db file scattered read'  THEN ROUND(CASE WHEN time_waited_micro/1000/1000 < 0 THEN NULL ELSE time_waited_micro/1000/1000 END) END) multi_secs_total
,      MAX(CASE WHEN event_name = 'db file scattered read'  THEN ROUND((time_waited_micro/1000)/NULLif(waits,0)) END) multi_avg
,      MAX(CASE WHEN event_name = 'db file parallel read'   THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END/duration) END) iops_prefch
,      MAX(CASE WHEN event_name = 'db file parallel read'   THEN ROUND(CASE WHEN time_waited_micro/1000/1000 < 0 THEN NULL ELSE time_waited_micro/1000/1000 END) END) prefch_secs_total
,      MAX(CASE WHEN event_name = 'db file parallel read'   THEN ROUND((time_waited_micro/1000)/NULLif(waits,0)) END) prefch_avg
,      MAX(CASE WHEN event_name = 'direct path read'        THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits  END/duration) END) iops_direct
,      MAX(CASE WHEN event_name = 'direct path read'        THEN ROUND(CASE WHEN time_waited_micro/1000/1000 < 0 THEN NULL ELSE time_waited_micro/1000/1000 END) END) direct_secs_total
,      MAX(CASE WHEN event_name = 'direct path read'        THEN ROUND((time_waited_micro/1000)/NULLif(waits,0)) END) direct_avg
,      MAX(CASE WHEN event_name = 'direct path read temp'   THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END/duration) END) iops_temp
,      MAX(CASE WHEN event_name = 'direct path read temp'   THEN ROUND(CASE WHEN time_waited_micro/1000/1000 < 0 THEN NULL ELSE time_waited_micro/1000/1000 END) END) temp_secs_total
,      MAX(CASE WHEN event_name = 'direct path read temp'   THEN ROUND((time_waited_micro/1000)/NULLif(waits,0)) END) temp_avg
FROM   io_stats io
GROUP BY 
       io.dbid
,      io.inst
,      io.b_day
,      io.b_snap
,      io.e_snap
,      io.b_time
,      io.e_time
,      io.duration
HAVING b_day NOT IN (6,7)
AND    inst = 2 
--AND b_snap = 18673
--AND    e_time = '17:00'
ORDER BY b_snap DESC;

WITH subq_snaps AS
     (SELECT dbid                dbid
      ,      instance_number     inst
      ,      snap_id             e_snap
      ,      lag(snap_id) over (partition by instance_number, startup_time order by snap_id) b_snap
      ,      TO_CHAR(begin_interval_time,'D') b_day
      ,      TO_CHAR(begin_interval_time,'DD-MON-YYYY HH24:MI') b_time
      ,      TO_CHAR(end_interval_time,'HH24:MI')   e_time
      ,    ((extract(day    from (end_interval_time - begin_interval_time))*86400)
          + (extract(hour   from (end_interval_time - begin_interval_time))*3600)
          + (extract(minute from (end_interval_time - begin_interval_time))*60)
          + (extract(second from (end_interval_time - begin_interval_time)))) duration
      FROM   dba_hist_snapshot)
,    io_stats AS
     (SELECT ss.*
     ,      bv.event_name
     ,      ev.time_waited_micro_fg - bv.time_waited_micro_fg time_waited_micro
     ,      ev.total_waits_fg       - bv.total_waits_fg       waits
     FROM   subq_snaps            ss
     ,      dba_hist_system_event bv
     ,      dba_hist_system_event ev
     WHERE  bv.dbid                   = ss.dbid
     AND    bv.snap_id                = ss.b_snap  
     AND    bv.instance_number        = ss.inst
     AND    bv.event_name            IN ('db file sequential read','direct path read','direct path read temp','db file scattered read','db file parallel read')
     AND    ev.dbid                   = ss.dbid
     AND    ev.snap_id                = ss.e_snap
     AND    ev.instance_number        = ss.inst
     AND    ev.event_id               = bv.event_id)
,   time_model AS
    (SELECT ss.*
     ,      bm.stat_name
     ,      em.value - bm.value value
     FROM   subq_snaps              ss
     ,      dba_hist_sys_time_model em  
     ,      dba_hist_sys_time_model bm  
     WHERE bm.dbid                   = ss.dbid
     AND   bm.snap_id                = ss.b_snap  
     AND   bm.instance_number        = ss.inst
     AND   em.dbid                   = ss.dbid
     AND   em.snap_id                = ss.e_snap
     AND   em.instance_number        = ss.inst
     AND   bm.stat_id                = em.stat_id)
SELECT ss.inst
,      ss.b_snap
,      ss.e_snap
,      ss.b_time
,      ss.e_time
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'DB time' THEN tm.value END)/1000000/60,2),'999999990.99')                   db_time
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'DB time' THEN tm.value END)/(ss.duration*1000000),1),'999999990.99')        aas
,      (SELECT round(average,2)
        FROM   dba_hist_sysmetric_summary sm
        WHERE  sm.dbid            = ss.dbid
        AND    sm.snap_id         = ss.e_snap
        AND    sm.instance_number = ss.inst
        AND    sm.metric_name     = 'Average Synchronous Single-Block Read Latency'
        AND    sm.group_id        = 2)                                                                                                                   assbl     
,      (SELECT round(average,2)
        FROM   dba_hist_sysmetric_summary sm
        WHERE  sm.dbid            = ss.dbid
        AND    sm.snap_id         = ss.e_snap
        AND    sm.instance_number = ss.inst
        AND    sm.metric_name     = 'Host CPU Utilization (%)'
        AND    sm.group_id        = 2)                                                                                                                   cpu_util
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'DB CPU' THEN tm.value END)/1000000,2),'999999990.99')                                      db_cpu
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'sql execute elapsed time' THEN tm.value END)/1000000,2),'999999990.99')                    sql_time
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'PL/SQL execution elapsed time' THEN tm.value END)/1000000,2),'999999990.99')               plsql_time
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'parse time elapsed' THEN tm.value END)/1000000,2),'999999990.00')                          parse_time
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'failed parse elapsed time' THEN tm.value END)/1000000,2),'999999990.99')                   failed_parse
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'hard parse (sharing criteria) elapsed time' THEN tm.value END)/1000000,2),'999999990.99')  hard_parse_sharing
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'RMAN cpu time (backup/restore)' THEN tm.value END)/1000000,2),'999999990.99')              rman_cpu
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'connection management call elapsed time' THEN tm.value END)/1000000,2),'999999990.99')     connection_mgmt
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'sequence load elapsed time' THEN tm.value END)/1000000,2),'999999990.99')                  sequence_load
,      TO_CHAR(ROUND(100*MAX(CASE WHEN tm.stat_name = 'DB CPU' THEN tm.value END) 
           / NULLIF(MAX(CASE WHEN tm.stat_name = 'DB time' THEN tm.value END),0),2),'999999990.99')                                           db_cpu_perc
,      TO_CHAR(ROUND(100*MAX(CASE WHEN tm.stat_name = 'sql execute elapsed time' THEN tm.value END)
           / NULLIF(MAX(CASE WHEN tm.stat_name = 'DB time' THEN tm.value END),0),2),'999999990.99')                                           sql_time_perc
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'PL/SQL execution elapsed time' THEN tm.value END)
           / NULLIF(MAX(CASE WHEN tm.stat_name = 'DB time' THEN tm.value END),0),2),'999999990.99')                                           plsql_time_perc
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'parse time elapsed' THEN tm.value END)
           / NULLIF(MAX(CASE WHEN tm.stat_name = 'DB time' THEN tm.value END),0),2),'999999990.99')                                parse_time_perc
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'failed parse elapsed time' THEN tm.value END)
           / NULLIF(MAX(CASE WHEN tm.stat_name = 'DB time' THEN tm.value END),0),2),'999999990.99')                                failed_parse_perc
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'hard parse (sharing criteria) elapsed time' THEN tm.value END)
           / NULLIF(MAX(CASE WHEN tm.stat_name = 'DB time' THEN tm.value END),0),2),'999999990.99')                                hard_parse_sharing_perc
,      TO_CHAR(ROUND(MAX(CASE WHEN tm.stat_name = 'RMAN cpu time (backup/restore)' THEN tm.value END)
           / NULLIF(MAX(CASE WHEN tm.stat_name = 'DB time' THEN tm.value END),0),2),'999999990.99')                                rman_cpu_perc
,      MAX(CASE WHEN event_name = 'db file sequential read' THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END) END) single_waits
,      MAX(CASE WHEN event_name = 'db file scattered read'  THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END) END) multi_waits
,      MAX(CASE WHEN event_name = 'db file parallel read'   THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END) END) prefch_wait
,      MAX(CASE WHEN event_name = 'direct path read'        THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits  END) END) direct_waits
,      MAX(CASE WHEN event_name = 'direct path read temp'   THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END) END)  temp_waits
,      MAX(CASE WHEN event_name = 'db file sequential read' THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END/ss.duration) END) iops_single
,      MAX(CASE WHEN event_name = 'db file sequential read' THEN ROUND(CASE WHEN time_waited_micro/1000/1000 < 0 THEN NULL ELSE time_waited_micro/1000/1000 END) END) single_secs_total
,      MAX(CASE WHEN event_name = 'db file sequential read' THEN ROUND((time_waited_micro/1000)/NULLif(waits,0)) END) single_avg
,      MAX(CASE WHEN event_name = 'db file scattered read'  THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END/ss.duration) END) iops_multi
,      MAX(CASE WHEN event_name = 'db file scattered read'  THEN ROUND(CASE WHEN time_waited_micro/1000/1000 < 0 THEN NULL ELSE time_waited_micro/1000/1000 END) END) multi_secs_total
,      MAX(CASE WHEN event_name = 'db file scattered read'  THEN ROUND((time_waited_micro/1000)/NULLif(waits,0)) END) multi_avg
,      MAX(CASE WHEN event_name = 'db file parallel read'   THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END/ss.duration) END) iops_prefch
,      MAX(CASE WHEN event_name = 'db file parallel read'   THEN ROUND(CASE WHEN time_waited_micro/1000/1000 < 0 THEN NULL ELSE time_waited_micro/1000/1000 END) END) prefch_secs_total
,      MAX(CASE WHEN event_name = 'db file parallel read'   THEN ROUND((time_waited_micro/1000)/NULLif(waits,0)) END) prefch_avg
,      MAX(CASE WHEN event_name = 'direct path read'        THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits  END/ss.duration) END) iops_direct
,      MAX(CASE WHEN event_name = 'direct path read'        THEN ROUND(CASE WHEN time_waited_micro/1000/1000 < 0 THEN NULL ELSE time_waited_micro/1000/1000 END) END) direct_secs_total
,      MAX(CASE WHEN event_name = 'direct path read'        THEN ROUND((time_waited_micro/1000)/NULLif(waits,0)) END) direct_avg
,      MAX(CASE WHEN event_name = 'direct path read temp'   THEN ROUND(CASE WHEN waits < 0 THEN NULL ELSE waits END/ss.duration) END) iops_temp
,      MAX(CASE WHEN event_name = 'direct path read temp'   THEN ROUND(CASE WHEN time_waited_micro/1000/1000 < 0 THEN NULL ELSE time_waited_micro/1000/1000 END) END) temp_secs_total
,      MAX(CASE WHEN event_name = 'direct path read temp'   THEN ROUND((time_waited_micro/1000)/NULLif(waits,0)) END) temp_avg
FROM  subq_snaps       ss
LEFT JOIN time_model   tm
ON   (tm.dbid        = ss.dbid
AND   tm.b_snap      = ss.b_snap  
AND   tm.inst        = ss.inst)
LEFT JOIN io_stats     io
ON   (io.dbid        = ss.dbid
AND   io.b_snap      = ss.b_snap  
AND   io.inst        = ss.inst)
GROUP BY 
       ss.dbid
,      ss.inst
,      ss.b_day
,      ss.b_snap
,      ss.e_snap
,      ss.b_time
,      ss.e_time
,      ss.duration
HAVING ss.b_day NOT IN (6,7)
--AND    inst = 2 
--AND b_snap = 18673
--AND    e_time = '17:00'
ORDER BY ss.b_snap DESC NULLS LAST;
