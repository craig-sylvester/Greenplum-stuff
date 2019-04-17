/****************************************************************************************
 * Check if table exists in the current schema
 * Usage: table_exists (<table>)
 * Returns: boolean
 ****************************************************************************************/
drop function if exists table_exists(varchar);
create function table_exists(tbl varchar)
returns boolean
as $$

    status = True

    # Verify that the source table exists
    sql = """select 1 from pg_catalog.pg_tables
             where tablename = '{0}'
               and schemaname = current_schema()""".format(tbl)
    try:
        if plpy.execute(sql).nrows() == 0:
            status = False
    except ValueError as e:
        status = False

    return status
$$
language plpythonu
;
-- END table_exists

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
        sql = """create index idx_{0} on {0}
                  using GIN(body jsonb_path_ops)""".format(doc_tbl)
        try:
            plpy.execute(sql)
        except:
            status = False

    if status is True:
        sql = """create index idx_{0}_search on {0}
                  using GIN(search)""".format(doc_tbl)
        try:
            plpy.execute(sql)
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
        value_list = []

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
                sql ="select id from {0} where id = {1}".format(plpy.quote_ident(doc_tbl), id)
                rv = plpy.execute(sql)
                if rv.nrows() == 0:
                    id = None
            except:
                id = None

            if id is None:
                sql = "select pde_get_search_vector({0}) as sv".format(
                         plpy.quote_literal(json.dumps(doc)))
                rv_search = plpy.execute(sql)
                searchVector = rv_search[0]['sv']

                onerow = "({0},to_tsvector({1}))".format(
                                           plpy.quote_literal(json.dumps(doc)),
                                           plpy.quote_literal(searchVector))
                value_list.append(onerow)

        ins_values = ','.join(value_list)
        sql = "insert into {0} (body,search) values {1}".format(
                                   plpy.quote_ident(doc_tbl),
                                   ins_values )
        try:
            rv = plpy.execute(sql)
            success_cnt = rv.nrows()
        except:
            return 'Insert failed'

            #sql = """update {0}
            #         set body = {1},
            #             search = to_tsvector({2}),
            #             updated_at = now()
            #         where id = {3} """.format(
            #                       plpy.quote_ident(doc_tbl),
            #                       plpy.quote_literal(searchVector),
            #                       plpy.quote_literal(json.dumps(doc)), id)
            #try:
            #    plpy.execute(sql)
            #    success_cnt = success_cnt + 1
            #except:
            #    ''

        return '{0} docs inserted/updated'.format(success_cnt)

    #
    # MAIN
    #

    # Check if the documents table exists.
    rv = plpy.execute("select table_exists('{0}') as status".format(doc_tbl))
    if rv[0]['status'] is False:
        return "Error: Source table {0} does not exist".format(doc_tbl)

    return insert_docs(doc_tbl, doc_string)

$$
language plpythonu
;
-- END pde_save_documents (from a list of text strings)

/****************************************************************************************
 * Import JSON text from a source table to the document table.
 * Usage: pde_load_documents (<document table>, <source table>, <source table column>)
 * Returns: text (number of successful and failed inserts)
 ****************************************************************************************/
drop function if exists pde_load_documents(varchar, varchar, varchar);
create function pde_load_documents(tbl varchar, source_tbl varchar, source_tbl_col varchar)
returns text
as $$
    import json
    import sys

    #
    # Function: Insert the JSON docs from the source table provided.
    # Usage: insert_docs (<table>, <source_tbl table>, <col from src tbl>)
    #     Returns: int (count of docs inserted/updated)
    #

    def insert_docs(doc_tbl, src, src_col):

        value_list = []
        status = None

        # Select the source JSON data we want to insert into our
        # document table.
        # NOTE: This is a dangerous operation since we are not
        #       limiting the number of rows being returned.
        #       In a production scenerio, we would likely want to
        #       do this in steps.
        try:
            sql = "select {0} from {1}".format(plpy.quote_ident(src_col),
                                               plpy.quote_ident(src) )
            rv_sel = plpy.execute(sql)
        except:
            return 'Error {0}: SQL = "{1}"'.format(sys.exc_info()[0], sql)

        nrows = len(rv_sel)
        for x in range(0, nrows):
            doc = json.loads(rv_sel[x][src_col])

            # Get the search vector for supporting FTS indexing
            sql = "select pde_get_search_vector({0}) as sv".format(plpy.quote_literal(json.dumps(doc)))
            rv_search = plpy.execute(sql)
            searchVector = rv_search[0]['sv']

            onerow = "({0}::jsonb,to_tsvector({1}))".format(
                                       plpy.quote_literal(json.dumps(doc)),
                                       plpy.quote_literal(searchVector))
            value_list.append(onerow)

        ins_values = ','.join(value_list)
        sql = "insert into {0} (body,search) values {1}".format(
                                   plpy.quote_ident(doc_tbl),
                                   ins_values )
        try:
            rv = plpy.execute(sql)
            status = '{0} docs inserted'.format(rv.nrows())
        except:
            status = 'Insert failed'

        return status

    #
    # MAIN
    #

    # Check if the documents table exists.
    rv = plpy.execute("select table_exists('{0}') as status".format(tbl))
    if rv[0]['status'] is False:
        return "Error: Target 'documents' table {0} does not exist".format(tbl)

    # Check if the source table exists.
    rv = plpy.execute("select table_exists('{0}') as status".format(source_tbl))
    if rv[0]['status'] is False:
        return "Error: Source table {0} does not exist".format(source_tbl)

    return insert_docs(tbl, source_tbl, source_tbl_col)

$$
language plpythonu
;
-- END pde_load_documents (import from source table)


/****************************************************************************************
 * Retrieve the values for the fields we may want to perform FTS on
 * Usage: pde_get_search_vector (<document>)
 * Returns: text (search vector)
 ****************************************************************************************/
drop function if exists pde_get_search_vector(varchar);
create function pde_get_search_vector(msg_body varchar)
returns text
as $$
    import json
    status = False
    searchFields = ["name", "first", "last", "first_name", "last_name"]
    searchFields.extend(["email", "home_email", "work_email", "description"])
    searchFields.extend(["company", "city", "country", "state", "addr", "addr1", "addr2"])
    searchFields.extend(["composer"])
    searchVal = []
    searchVector = ""

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


    doc = json.loads(msg_body)
    iter_thru_doc(doc)

    if len(searchVal) > 0:
        searchVector = " ".join(searchVal).encode('UTF-8')
        
    return searchVector
$$
language plpythonu
;
-- END pde_get_search_vector

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

    sql = "select body from {0} where id = {1}".format(plpy.quote_ident(tbl), id)
    rv = plpy.execute(sql)
    if len(rv) == 1:
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
    for x in range(0,  len(rv)):
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
    doc = []

    # Check for valid table

    if table_exists(tbl):
        sql = """select body, ts_rank_cd(search, to_tsquery({0})) as rank
                 from {1}
                 where search @@ to_tsquery({0})
                 order by rank desc""".format(
                            plpy.quote_literal(query),
                            plpy.quote_ident(tbl) )
        rv = plpy.execute(sql)
        for x in range(0,  rv.nrows()):
            doc.append (rv[x]['body'])
    else:
        doc.append('Table "{0}" not found'.format(tbl))

    return doc
$$
language plpythonu
;
-- END pde_search_documents
