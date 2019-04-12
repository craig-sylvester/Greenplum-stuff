#!/usr/bin/env bash

# Load the JSON document functions

default_db=${PGDATABASE:-$(id -un)}

read -p "Enter database to load functions into (default = '$default_db'): " db
[[ -z $db ]] && db=$default_db

psql -d $db -f json_doc_funcs.sql
