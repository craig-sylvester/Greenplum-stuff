--
-- Name: show_table_sizes(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE OR REPLACE FUNCTION show_table_sizes(IN schema_name text,
      OUT table_name text,
      OUT size_all bigint, OUT size_main bigint, OUT size_toast bigint,
      OUT size_index bigint, OUT size_all_pretty text, OUT size_main_pretty text,
      OUT size_toast_pretty text, OUT size_index_pretty text)
RETURNS SETOF record
AS $$

DECLARE

  query_str       TEXT;
  loop_record     RECORD;
  query2_str      TEXT;
  loop2_record    RECORD;
  query3_str      TEXT;
  loop3_record    RECORD;
  toast_size_main BIGINT;
  toast_size_all  BIGINT;

BEGIN

  -- get all tables
  query_str := format('SELECT * FROM information_schema.tables
                        WHERE table_type = ''BASE TABLE'' AND table_schema = %L
                        ORDER BY table_name',
                       schema_name);
  FOR loop_record IN EXECUTE query_str LOOP
      /************************************
         pg_relation_size(text): Disk space used by the table or index with the specified name.
         pg_total_relation_size(text): Total disk space used by the table with the
                                       specified name, including indexes and toasted data.
       ************************************ */

      query2_str := format( 'SELECT pg_relation_size(''%1s.%2s'')       AS size_main,
                                    pg_total_relation_size(''%3s.%4s'') AS size_all',
                    loop_record.table_schema, loop_record.table_name,
                    loop_record.table_schema, loop_record.table_name);
      EXECUTE query2_str INTO loop2_record;
      table_name := loop_record.table_name;

      size_main := loop2_record.size_main;
      size_all := loop2_record.size_all;

      -- get data for toast table
      query3_str := format('SELECT reltoastrelid::regclass AS toast_table FROM pg_class
                            WHERE oid = ''%1s.%2s''::regclass',
                           loop_record.table_schema, loop_record.table_name);
      EXECUTE query3_str INTO loop3_record;
      IF loop3_record.toast_table IS NOT NULL AND loop3_record.toast_table::TEXT != '-' THEN
        query3_str := format('SELECT pg_relation_size(%L) AS size_toast_main,
                                     pg_total_relation_size(%L) AS size_toast_all',
                              loop3_record.toast_table, loop3_record.toast_table);
        EXECUTE query3_str INTO loop3_record;
        toast_size_main := loop3_record.size_toast_main;
        toast_size_all := loop3_record.size_toast_all;
      ELSE
        toast_size_main := 0;
        toast_size_all := 0;
      END IF;

      size_toast := toast_size_main;
      size_index := size_all - size_main - toast_size_main;

      query3_str := format('SELECT pg_size_pretty(%s::bigint) AS size_main_pretty,
                                   pg_size_pretty(%s::bigint) AS size_all_pretty,
                                   pg_size_pretty(%s::bigint) AS size_toast_pretty,
                                   pg_size_pretty(%s::bigint) AS size_index_pretty',
                            size_main, size_all, size_toast, size_index
                           );
      EXECUTE query3_str INTO loop3_record;
      size_main_pretty := loop3_record.size_main_pretty;
      size_all_pretty := loop3_record.size_all_pretty;
      size_toast_pretty := loop3_record.size_toast_pretty;
      size_index_pretty := loop3_record.size_index_pretty;

      RETURN NEXT;
  END LOOP;

  RETURN;
END;
$$
    LANGUAGE plpgsql;


-- ALTER FUNCTION public.show_table_sizes(OUT schema_name text, OUT table_name text, OUT size_all bigint, OUT size_main bigint, OUT size_toast bigint, OUT size_index bigint, OUT size_all_pretty text, OUT size_main_pretty text, OUT size_toast_pretty text, OUT size_index_pretty text) OWNER TO postgres;
