#!/usr/bin/env python

"""
Extract GPDB release note data from the docs website and insert the information into
tables for searching purposes. This is mostly a personal project to learn a bit more
Python and to learn about the new capabilities in GPText 2.x.

The module depends on the existance of two tables in the gpdb_info schema:
release_info
   release_id    bigint
   release_url   varchar
   release_date  varchar
   release_notes text (full text of the release notes)

notes_by_topic
   release_id    bigint (not unique - GPText requires a key to be a bigint)
   topic         varchar
   topic_notes   text (text for this topic)
   id            bigint sequence (to uniquely id record for GPText)

The schema and table name are provided as pre-defined variables below so feel free to modify.

A note about the BASE_URL pre-defined variable. It is set to the base URL as of the 4.3.10 release.
To capture new release notes, you may need to modify to the current release.
"""

import codecs
import sys
sys.path.append('.')
from optparse import OptionParser
import re
import urllib2
from bs4 import BeautifulSoup as bs
import psycopg2
import parse_rel_notes

BASE_URL = "http://gpdb.docs.pivotal.io/43110/"
SCHEMA = "release_notes"
INFO_TBL = "release_info"
TOPICS_TBL = 'notes_by_topic'
testing = False
verbose = False

release_info_data = []       # holds the information we will add to the release info table
release_topics_data = []     # holds the information we will add to the release notes table
list_of_ids = []             # list of previously retrieved release notes

###########################################################################################
def get_options():
    parser = OptionParser( usage="%prog [options]" )
    parser.add_option ("-t", "--testing",
                        action="store_true", dest="testing", default=False,
                        help="test with only 3 releases (also sets verbose)")
    parser.add_option ("-v", "--verbose",
                        action="store_true", dest="verbose", default=False,
                        help="let you know what the hell is happening")
    (opts, args) = parser.parse_args()

    if opts.testing:
        opts.verbose = True
    return opts.testing, opts.verbose

###########################################################################################
# Open a cursor to perform database operations
def get_gpdb_cursor():
    conn = psycopg2.connect(host="craighp", port="5432",
                            dbname="craig", user="craig", password="craig")
    conn.set_session(autocommit=True)
    return conn.cursor()

###########################################################################################
# Retrieve the list of release IDs that have already been processed
def get_release_ids(curr_cur):
    id_list = []
    select_query = 'SELECT release_id FROM ' + SCHEMA + '.' + INFO_TBL + ' order by 1'
    try:
        curr_cur.execute(select_query)
        if curr_cur.rowcount:
            for x in curr_cur.fetchall():
                id_list.append(str(x[0])) # convert to string for symmetric_difference comparison
    except psycopg2.Error as e:
        print e.pgerror

    return id_list

###########################################################################################
# Insert the info list tuples into the db
def insert_into_release_info(curr_cur, info_data):
    recListTemplate = ','.join(['%s'] * len(info_data))
    insert_query = 'INSERT INTO ' + SCHEMA + '.' + INFO_TBL + ' VALUES {0}'.format(recListTemplate)
    try:
        curr_cur.execute(insert_query, info_data)
        if verbose:
            #print curr_cur.mogrify(insert_query, info_data)
            print '>>>>> Inserted data for ', curr_cur.rowcount, ' entries <<<<<'
    except psycopg2.Error as e:
        print e.pgerror

###########################################################################################
# Insert the note list tuples into the db
def insert_into_notes_by_topic(curr_cur, topics_list):
    recListTemplate = ','.join(['%s'] * len(topics_list))
    insert_query = 'INSERT INTO ' + SCHEMA + '.' + TOPICS_TBL + ' VALUES {0}'.format(recListTemplate)
    try:
        curr_cur.execute( insert_query, topics_list )
        if verbose:
            #print curr_cur.mogrify(insert_query, topics_dict.values())
            print '>>>>> Inserted data for ', curr_cur.rowcount, ' entries <<<<<'
    except psycopg2.Error as e:
        print e.pgerror

###########################################################################################

def retrieve_GPDB_release_list():
    RELEASE_INDEX = BASE_URL + "/index.html"
    # Retrieve the list of GPDB release notes available
    url = urllib2.Request(RELEASE_INDEX)
    try: response = urllib2.urlopen(url)
    except URLError as e:
        print "Failed to open URL: ", url
        print e.reason
        exit
    # Get the list of releases
    html_index = response.read()
    gpdb_rel_list = re.findall(r"Greenplum Database (.+) Release Notes", html_index)
    gpdb_rel_list = sorted(set(gpdb_rel_list))
    return gpdb_rel_list

###########################################################################################
def print_output_files(release, html_text, plain_text, topic_notes):
    f = open (release + '.html', 'w')
    f.write(html_text)
    f.close()

    f = open (release + '.out', 'w')
    f.write(plain_text)
    f.close()

    if isinstance(topic_notes, dict):
        f = open (release + '.topics.out', 'w')
        for topic in topic_notes:
            f.write('*' * 15)
            f.write( topic_notes[topic] )
        f.close()

###########################################################################################
def print_rows(data_rows, out_file):
    f = open ('gpdb_data.csv', 'w')
    for v in data_rows:
        if out_file:
            f.write('%s~%s~%s\n' % ( v[0],v[1],v[2]) )
            f1 = open (v[0] + '.out', 'w')
            f1.write(v[3])
            f1.close()
        else:
            f.write('%s~%s~%s~%s\n' % ( v[0],v[1],v[2],v[3]) )
    f.close()

###########################################################################################
###########################################################################################
###########################################################################################

testing, verbose = get_options()

gpdb_releases = retrieve_GPDB_release_list() # retrieves ID list from Docs website
gpdb_releases = [val.replace('.', '').ljust(4,'0')  for val in gpdb_releases]

# retrieves ID list from database (i.e., already exists)
gpdb_cur = get_gpdb_cursor()
list_of_ids = get_release_ids(gpdb_cur)

# Get the difference between the two ID lists
gpdb_releases = list(set(gpdb_releases).symmetric_difference(set(list_of_ids)) )
# If we set the testing flag, override the list of IDs for testing
if testing:
    gpdb_releases = ['4300', '4310', '4362']

if gpdb_releases:
    print 'Process releases: \n' , gpdb_releases

for release in gpdb_releases:

    url = BASE_URL + "/relnotes/GPDB_" + release + "_README.html"
    request = urllib2.Request(url)
    try: response = urllib2.urlopen(request)
    except urllib2.URLError as e:
        print "Failed to open URL: ", url
        print e.reason
        continue

    print 'Processing ', url

    html_text = response.read()

    # parse_rel_notes(html) returns: string, string, dictionary
    release_notes, rel_date, topic_notes = parse_rel_notes.parse_rel_note(html_text)

    if testing:
        print_output_files(release, html_text, release_notes, topic_notes)

    release_info_data.append( (release, url, rel_date, release_notes) )
    for key in topic_notes:
        release_topics_data.append( (release, key, topic_notes[key] ) )
    if verbose:
        print "Added: ", release, ", ", rel_date, ", ", url
# end loop  => for release in gpdb_releases:

if len(release_info_data):
    insert_into_release_info (gpdb_cur, release_info_data)
    insert_into_notes_by_topic (gpdb_cur, release_topics_data)

# print_rows(release_info_data, False)
