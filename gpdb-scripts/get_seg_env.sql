DROP EXTERNAL TABLE seg_env;
CREATE EXTERNAL WEB TABLE seg_env
  (
    cid int
   ,database text
   ,exec_date text
   ,master text
   ,master_port int
   ,seg_datadir text
   ,seg_pg_conf text
   ,seg_port int
   ,seg_cnt int
   ,seg_id int
   ,session_id int
   ,sn int
   ,exec_time text
   ,exec_user text
   ,xid text
  )
EXECUTE 'echo -e $GP_CID"\t"$GP_DATABASE"\t"$GP_DATE"\t"$GP_MASTER_HOST"\t"$GP_MASTER_PORT"\t"$GP_SEG_DATADIR"\t"$GP_SEG_PG_CONF"\t"$GP_SEG_PORT"\t"$GP_SEGMENT_COUNT"\t"$GP_SEGMENT_ID"\t"$GP_SESSION_ID"\t"$GP_SN"\t"$GP_TIME"\t"$GP_USER"\t"$GP_XID'
ON ALL FORMAT 'text'
;
