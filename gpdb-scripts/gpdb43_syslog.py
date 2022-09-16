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
select_list = (
  'logtime, ',
  'loguser, ',
  'logdatabase, ',
  'logpid, ',
  'logthread, ',
  'loghost, ',
  'logport, ',
  'logsessiontime, ',
  'logtransaction, ',
  'logsession, ',
  'logcmdcount, ',
  'logsegment, ',
  'logslice, ',
  'logdistxact, ',
  'loglocalxact, ',
  'logsubxact, ',
  'logseverity, ',
  'logstate, ',
  'logmessage, ',
  'logdetail, ',
  'loghint, ',
  'logquery, ',
  'logquerypos, ',
  'logcontext, ',
  'logdebug, ',
  'logcursorpos, ',
  'logfunction, ',
  'logfile, ',
  'logline, ',
  'logstack '
)
sqlcommand = 'select ' + ''.join(select_list) + ' from gp_toolkit.gp_log_system;'

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
    logsegment      = row[11]
    logslice        = row[12]
    logdistxact     = row[13]
    loglocalxact    = row[14]
    logsubxact      = row[15]
    logseverity     = row[16]
    logstate        = row[17]
    logmessage      = row[18]
    logdetail       = row[19]
    loghint         = row[20]
    logquery        = row[21]
    logquerypos     = row[22]
    logcontext      = row[23]
    logdebug        = row[24]
    logcursorpos    = row[25]
    logfunction     = row[26]
    logfile         = row[27]
    logline         = row[28]
    logstack        = row[29]

    output_message = "GPDB SYSLOG: %s %s %s %s %s %s %s %s %s %s" % (logtime, loguser, logdatabase, loghost, logport, logsegment, logstate, logmessage, logdetail, logquery)
    syslog.syslog(syslog.LOG_ERR, output_message)
 
