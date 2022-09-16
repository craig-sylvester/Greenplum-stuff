#!/usr/bin/env python

import syslog, sys
from gppylib.db.dbconn import connect, DbURL, execSQL
from gppylib.gpparseopts import OptParser, OptChecker


parser = OptParser(option_class=OptChecker)
parser.remove_option('-h')
parser.add_option('-h', '--host', type='string')
parser.add_option('-p', '--port', type='string')
parser.add_option('-d', '--db', type='string')
parser.add_option('-u', '--user', type='string')
(options, args) = parser.parse_args()

if (not options.host or not options.port or not options.db or not options.user):
    print "gpdb_syslog -h HOST -p PORT -d DB -u USER"
    sys.exit(1)

url = DbURL(options.host, int(options.port), options.db, options.user)
conn = connect(url)
sqlcommand = 'select * from public.logs_extract;'
cur = execSQL(conn, sqlcommand)
for row in cur:
    logtime         = row[0]
    loguser         = row[1]
    logdatabase     = row[2]
    logpid          = row[3]
    logthread       = row[4]
    loghost         = row[5]
    logport         = row[6]
    logsessiontime  = row[7]
    logtransaction  = row[8]
    logsession      = row[9]
    logcmdcount     = row[10]
    logsegment      = row[12]
    logslice        = row[13]
    logdistxact     = row[14]
    loglocalxact    = row[15]
    logsubxact      = row[16]
    logseverity     = row[17]
    logstate        = row[18]
    logmsg          = row[19]
    logdetail       = row[20]
    loghint         = row[21]
    logquery        = row[22]
    logquerypos     = row[23]
    logcontext      = row[24]
    logdebug        = row[25]
    logcursorpos    = row[26]
    logfunction     = row[27]
    logfile         = row[28]
    logline         = row[29]

    output_message = "GPDB SYSLOG: %s %s %s %s %s %s %s %s %s %s" % (logtime, loguser, logdatabase, loghost, logport, logsegment, logstate, logmsg, logdetail, logquery)
    syslog.syslog(syslog.LOG_ERR, output_message)
 
