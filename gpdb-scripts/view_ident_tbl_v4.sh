#!/usr/bin/env bash

usage="$0 <dbname>"
[[ $# != 1 ]] && { echo $usage; exit 1; }
DB=$1
TEMP=temp.sql

DB_OID=$(psql -d $DB -Atc 'SELECT oid FROM pg_database WHERE datname = current_database()')
[[ $? != 0 ]] && { echo "Failed to get OID for db '$1'"; exit 1; }

echo "OID for db '$DB' is '$DB_OID'"

cat <<__EOF > ${TEMP}
DROP VIEW IF EXISTS v_ident_table_last_change cascade;

DROP EXTERNAL TABLE IF EXISTS ext_db_files_last_change;

CREATE EXTERNAL WEB TABLE ext_db_files_last_change 
(
    segment_id INTEGER,
    relfilenode TEXT,
    filename TEXT,
    size NUMERIC,
    last_timestamp_change timestamp 
)
EXECUTE 'find \$GP_SEG_DATADIR/base/${DB_OID} -type f -printf "\$GP_SEGMENT_ID|%f|%h/%f|%s|%TY-%Tm-%Td %TX\n" 2> /dev/null || true' ON ALL
FORMAT 'text' (delimiter '|' null '\N' escape '\\')
;

CREATE OR replace VIEW v_ident_table_last_change
   AS
      WITH last_change 
         AS (SELECT split_part(relfilenode, '.', 1) AS table_relfilenode, 
                    SUM(SIZE)                             AS SIZE, 
                    Max(last_timestamp_change)            AS max_last_timestamp_change 
             FROM   ext_db_files_last_change 
             WHERE  relfilenode NOT IN ( 'PG_VERSION', 'pg_internal.init' ) 
             GROUP  BY 1)
      SELECT (n.nspname ||'.'|| tab.relname ) AS relation_name, 
             T.relpages, 
             T.reltuples,
             T.max_last_timestamp_change,
             pslo.statime AS last_analyze_timestamp,
             tab.relhassubclass AS flag_is_partitioned,
             CASE 
               WHEN tab.relhassubclass = FALSE
                 THEN max_last_timestamp_change
               ELSE Greatest(Max(max_last_timestamp_change)
                             over
                               ( PARTITION BY
                                CASE WHEN tab.relhassubclass = TRUE
                                THEN n.nspname
                                ELSE part.schemaname
                                END,
                                CASE WHEN tab.relhassubclass = TRUE
                                THEN tab.relname
                                ELSE part.tablename
                                END
                               ), max_last_timestamp_change
                             )
               END  AS global_max_last_timestamp_change,
		 CASE
		   WHEN tab.relhassubclass = FALSE
		     THEN sum_size_table 
		     ELSE SUM(sum_size_table)
			  over (
			    PARTITION BY
			    CASE WHEN tab.relhassubclass = TRUE
			       THEN n.nspname
			       ELSE part.schemaname
			    END,
			    CASE WHEN tab.relhassubclass = TRUE
			       THEN tab.relname
			       ELSE part.tablename
			    END
			  )
		 END   AS global_sum_size_table,
		 T.sum_size_table,
		 tab.relkind  AS relkind,
		 tab.relstorage,
		 CASE
		   WHEN tab.relhassubclass = TRUE THEN n.nspname ELSE part.schemaname
		 END AS part_master_schema,
		 CASE
		   WHEN tab.relhassubclass = TRUE THEN tab.relname ELSE part.tablename 
		 END AS part_master_table
	  FROM   (SELECT Coalesce(aoseg.relid, aovisi.relid, toast.oid, c.oid) AS oid
			,SUM(c.relpages)  AS relpages
			,SUM(c.reltuples) AS reltuples
			,Max(last_change.max_last_timestamp_change)  AS max_last_timestamp_change
			,SUM(last_change.SIZE)   AS sum_size_table
		  FROM  last_change
			 inner join pg_class c
				 ON last_change.table_relfilenode = c.relfilenode::text
			 left outer join pg_appendonly aoseg
				 ON aoseg.segrelid = c.oid
			 left outer join pg_appendonly aovisi
				 ON aovisi.visimaprelid = c.oid
			 left outer join pg_class toast
				 ON c.oid = toast.reltoastrelid
		  GROUP  BY 1) T
		 inner join pg_class tab
		       ON T.oid::text = tab.oid::text
		 left join pg_stat_last_operation pslo
		       ON pslo.objid = T.oid
		       AND pslo.staactionname = 'ANALYZE'
		 inner join pg_namespace n
		       ON tab.relnamespace = n.oid
			    AND n.nspname NOT IN ( 'information_schema', 'pg_catalog', 'pg_toast')
		 left outer join pg_partitions part
		       ON n.nspname = part.partitionschemaname
			    AND tab.relname = part.partitiontablename
;
__EOF

psql -d $DB -f ${TEMP}
[[ $? == 0 ]] && rm -f ${TEMP}
