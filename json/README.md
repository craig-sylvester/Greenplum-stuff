A set of custom UDFs (User Defined Functions) for loading
into Postgresql 9.4 (or greater) or
Greenplum 6.x (or greater) that provide for a more
JSON "document store" like experience for a developer.
The functions are written in Python 2.7 (except for the  `table_exists` function).

The functions provided:
- `table_exists(tbl varchar)` : check if the table exists
- `pde_create_document_table(doc_tbl varchar, out varchar)` : create the document table and two GIN indices for searching
- `pde_replace_document (doc_tbl varchar, doc_id varchar, doc_text text)` : replace an existing document based on the document id
- `pde_save_documents (doc_tbl varchar, doc_string text)` : save a set of documents. Expects a Python list as the doc_string.
- `pde_load_documents(tbl varchar, source_tbl varchar, source_tbl_col varchar)` : load documents from an existing table
- `pde_get_search_vector(msg_body varchar)` : retrieve the values for the fields we may want to perform a FTS (full text search) on
- `pde_find_document(tbl varchar, id int)` : find a document by id
- `pde_find_document(tbl varchar, criteria json, orderby varchar default 'id')` : find document(s) by a json key field
- `pde_search_documents(tbl varchar, query varchar)` : find document(s) by a FTS query

To load, simply run `psql -d <your database> -f json_doc_funcs.sql` or run the `load_json_functions.sh` script.

The `example-data` directory contains SQL load files for facilitating a quick test.
