#!/usr/bin/env python
#
# Read in a list/array of JSON documents from a file and write out a new
# file in the format of one JSON document per line.
# We also filter the text to remove characters that give the Postgres/GP
# JSON parsers problems, i.e.:
#   - remove double quotes (") embedded in an object
#   - remove backslashes (\) embedded in an object

import json
import sys

if len(sys.argv) > 1:
    files = sys.argv[1:]
else:
    files = raw_input ("Enter file name(s) to convert: ")
    files = files.split()

for this_file in files:
    docs = json.load(open(this_file))

    outname = this_file + '.out'
    with open(outname, 'w') as newfile:
        for item in docs:
            item_str = json.dumps(item)
            item_str = item_str.replace('\\"','~').replace('\\','')
            print >> newfile, item_str
        print 'Input file: {0}, output file: {1}'.format(this_file, outname)
