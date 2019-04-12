/****************************************************************************************
 * Create Document Table
 * Two indices are also created:
 *  1. GIN index on the JSON data
 *  2. GIN index on the "search" vector
 * Usage: pde_create_document_table (<document table>)
 * Returns: boolean (success or failure)
 ****************************************************************************************/

drop function if exists pde_create_document_table(varchar, out boolean);
create function pde_create_document_table(doc_tbl varchar, out boolean)
as $$
    status = True

    sql = """create table %s  ( 
               id serial primary key
               , body jsonb not null
               , search tsvector
               , created_at timestamptz default now() not null 
               , updated_at timestamptz default now() not null
               )"""
    try:
        plpy.execute(sql % doc_tbl)
    except:
        status = False

    if status is True:
        sql = "create index idx_" + doc_tbl + " on " + doc_tbl + " using GIN(body jsonb_path_ops)"
        try:
            plpy.execute(sql);
        except:
            status = False

    if status is True:
        sql = "create index idx_" + doc_tbl + "_search on " + doc_tbl + " using GIN(search)"
        try:
            plpy.execute(sql);
        except:
            status = False

    return status
$$
language plpythonu
;
-- END pde_create_document_table

/****************************************************************************************
 * Save JSON documents to document table
 * Usage: pde_save_documents (<document table>, <list of JSON docs>)
 * Returns: text (number of successful and failed inserts)
 ****************************************************************************************/
drop function if exists pde_save_documents (varchar, text);
create function pde_save_documents (doc_tbl varchar, doc_string text)
returns text
as $$

    import json

    #
    # Function: Insert the list of JSON docs into the table provided.
    #

    def insert_docs(doc_tbl, theDocs):

        success_cnt = 0
        failure_cnt = 0

        # Verify that we received a JSON string
        try:
            doc_list = json.loads(theDocs)
        except ValueError as e:
            return 'ERROR: ill-formed JSON. Check syntax.'

        # Verify that a Python LIST was passed in
        if not isinstance(doc_list, list):
            return 'ERROR: Python LIST of JSON docs expected'

        for doc in doc_list:
            try:
                id = doc['id']
            except:
                id = None

            # Check if the document has an "id" field that does not exist in the table. If so,
            # set it to None/null so that we INSERT the doc.
            if id:
                rv = plpy.execute("select id from {0} where id = {1}".format(plpy.quote_ident(doc_tbl), id))
                if rv.nrows() == 0:
                    id = None

            if id == None:
                rv = plpy.execute("insert into {0} (body) values ({1}) returning *".format(
                                           plpy.quote_ident(doc_tbl),
                                           plpy.quote_literal(json.dumps(doc)) ) )
                id = rv[0]['id']
                doc['id'] = id

            plpy.execute("update {0} set body = {1}, updated_at = now() where id = {2} returning *".format(
                                       plpy.quote_ident(doc_tbl),
                                       plpy.quote_literal(json.dumps(doc)), id) )

            try:
                plpy.execute("select pde_update_search({0}, {1})".format(plpy.quote_literal(doc_tbl), id))
                success_cnt = success_cnt + 1
            except:
                failure_cnt = failure_cnt + 1


        return '{0} documents inserted/updated : {1} documents had errors'.format(success_cnt, failure_cnt)

    #
    # MAIN
    #

    # Check if table exists.
    #
    sql = "select 1 from pg_catalog.pg_tables where tablename = {0} and schemaname = current_schema()".format( plpy.quote_literal(doc_tbl) )
    if plpy.execute(sql).nrows() == 0:
        return "Error: Target 'documents' table {0} does not exist".format(doc_tbl)

    return insert_docs(doc_tbl, doc_string)

$$
language plpythonu
;
-- END pde_save_documents (from a list of text strings)

/****************************************************************************************
 * Import JSON text from a source table to the document table.
 * Usage: pde_save_documents (<document table>, <source table>, <source table column>)
 * Returns: text (number of successful and failed inserts)
 ****************************************************************************************/
drop function if exists pde_save_documents(varchar, varchar, varchar);
create function pde_save_documents(tbl varchar, source_tbl varchar, source_tbl_col varchar)
returns text
as $$

    import json
    import sys

    #
    # Function: Insert the list of JSON docs into the table provided.
    # Usage: insert_docs (<table>, <source_tbl table>, <col from src tbl>)
    #     Returns: int (count of docs inserted/updated)
    #

    def insert_docs(tbl, src, src_col):

        status = None
        success_cnt = 0
        failure_cnt = 0

        # Verify that the source table exists
        try:
            sql = "select 1 from pg_catalog.pg_tables where tablename = {0} and schemaname = current_schema()".format( plpy.quote_literal(src) )
            if plpy.execute(sql).nrows() == 0:
                status = "Error: Source table '{0}' does not exist".format(src)
        except ValueError as e:
            status = 'plpy.execute error'

        # Check if the document has an "id" field that does not exist in the table. If so,
        # set it to None/null so that we INSERT the doc.

        if status is None:
            try:
                sql = "select {0} from {1}".format(plpy.quote_ident(src_col),plpy.quote_ident(src))
                rv = plpy.execute(sql)
            except:
                return 'Error on {1}: {0}'.format(sys.exc_info()[0], sql)

            nrows = len(rv) - 1
            for x in range(0, nrows):
                try:
                    doc = rv[x][src_col]
                except:
                    return 'Failed on row {0} of {2}: {1}'.format(x,sys.exc_info()[0], nrows)
                    #return 'Failed on row {0}: {1}'.format(x,doc)
               

                rv2 = plpy.execute("insert into {0} (body) values ({1}::jsonb) returning id, body".format(
                                           plpy.quote_ident(tbl),
                                           plpy.quote_literal(doc) ) )
                id = rv2[0]['id']
                doc = json.loads(rv2[0]['body'])
                doc['id'] = id

                #status = 'doc type: {0}, id : {1}'.format(type(doc),id)

                plpy.execute("update {0} set body = {1}, updated_at = now() where id = {2} returning *".format(
                                           plpy.quote_ident(tbl),
                                           plpy.quote_literal(json.dumps(doc)),
                                           id
                                          ) )

                try:
                    plpy.execute("select pde_update_search({0}, {1})".format(plpy.quote_literal(tbl), id))
                    success_cnt = success_cnt + 1
                except:
                    failure_cnt = failure_cnt + 1

        status = '{0} documents inserted/updated : {1} documents had errors'.format(success_cnt, failure_cnt)

        return status

    #
    # MAIN
    #

    # Check if table exists.
    sql = "select 1 from pg_catalog.pg_tables where tablename = {0} and schemaname = current_schema()".format( plpy.quote_literal(tbl) )
    if plpy.execute(sql).nrows() == 0:
        return "Error: Target 'documents' table {0} does not exist".format(tbl)

    return insert_docs(tbl, source_tbl, source_tbl_col)

$$
language plpythonu
;
-- END pde_save_documents (import from source table)


/****************************************************************************************
 * Update the "search" field in the document table
 * Usage: pde_update_search (<document table>, <id of JSON doc>)
 * Returns: boolean (success or failure)
 ****************************************************************************************/
drop function if exists pde_update_search(varchar, int);
create function pde_update_search(tbl varchar, id int)
returns boolean
as $$
    import json
    status = False
    searchFields = ["name", "first", "last", "first_name", "last_name"]
    searchFields.extend(["email", "home_email", "work_email", "description"])
    searchFields.extend(["company", "city", "country", "state", "addr", "addr1", "addr2"])
    searchFields.extend(["composer"])
    searchVal = []

    def iter_thru_doc(obj):
        for key,value in obj.items():
            if key in searchFields and value not in searchVal and value is not None:
                searchVal.append(value)
            if type(value) == type(dict()):
                iter_thru_doc(value)
            elif type(value) == type(list()):
                for val in value:
                    if type(val) == type(str()):
                        pass
                    elif type(val) == type(list()):
                        pass
                    else:
                        iter_thru_doc(val)


    sql = "select body from {0} where id = {1}".format(plpy.quote_ident(tbl), id)
    rv = plpy.execute(sql)
    if rv.nrows() == 1:
        doc = json.loads(rv[0]['body'])
        searchVal = []
        iter_thru_doc(doc)

        if len(searchVal) > 0:
            searchVector = " ".join(searchVal).encode('UTF-8')
            try:
                plpy.execute("update {0} set search = to_tsvector({1}) where id = {2}".format(
                                            plpy.quote_ident(tbl),
                                            plpy.quote_literal( searchVector ), id))
                status = True
            except:
                status = False
        
    return status
$$
language plpythonu
;
-- END pde_update_search

/****************************************************************************************
 * Find document by document-id in document table
 * Usage: pde_find_document (<document table>, <id>)
 * Returns: text (JSON document if found, otherwise '{}')
 ****************************************************************************************/

drop function if exists pde_find_document(varchar, int);
create function pde_find_document(tbl varchar, id int)
returns text
as $$
    import json
    status = False

    sql = "select * from {0} where id = {1}".format(plpy.quote_ident(tbl), id)
    rv = plpy.execute(sql)
    if rv.nrows() == 1:
        doc = json.loads(rv[0]['body'])
    else:
        doc = json.loads('{}')

    return doc
$$
language plpythonu
;
-- END pde_find_document

/****************************************************************************************
 * Find document by JSON key field in document table
 * Usage: pde_save_documents (<document table>, <criteria in JSON format>,
 *                            [orderby field: default is 'id'])
 * Returns: setof text (text of JSON documents found)
 ****************************************************************************************/

drop function if exists pde_find_document(varchar, jsonb, varchar);
create function pde_find_document(tbl varchar, criteria jsonb, orderby varchar default 'id')
returns setof text
as $$
    import json
    status = False
    doc = []

    sql = "select body from {0} where body @> {1} order by body ->> {2}".format(
                        plpy.quote_ident(tbl),
                        plpy.quote_literal(criteria),
                        plpy.quote_literal(orderby) ) 
    rv = plpy.execute(sql)
    for x in range(0,  len(rv) - 1):
        doc.append (rv[x]['body'])

    return doc
$$
language plpythonu
;
-- END pde_find_document

/****************************************************************************************
 * Find document by FTS (full text search) query in selected table
 * Usage: pde_save_documents (<document table>, <query>)
 * Returns: setof text (text of JSON documents found)
 ****************************************************************************************/

drop function if exists pde_search_documents(varchar, varchar);
create function pde_search_documents(tbl varchar, query varchar)
returns setof text
as $$
    import json
    status = False
    doc = []

    # Check for valid table

    sql = "select table_name from information_schema.tables where table_name = " + plpy.quote_literal(tbl)
    if plpy.execute(sql).nrows() == 0:
        doc.append('Table "{0}" not found'.format(tbl))
    else:
        sql = "select body, ts_rank_cd(search, to_tsquery({0})) as rank from {1} where search @@ to_tsquery({0}) order by rank desc".format(
                            plpy.quote_literal(query),
                            plpy.quote_ident(tbl) )
        rv = plpy.execute(sql)
        for x in range(0,  rv.nrows()):
            doc.append (rv[x]['body'])

    return doc
$$
language plpythonu
;
-- END pde_search_documents
