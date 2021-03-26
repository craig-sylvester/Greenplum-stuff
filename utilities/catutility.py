'''
Created on Aug 26, 2015

@author: mberendsen

Modified on Mar 26, 2021
by Craig Sylvester
- Added check for GP version before query of gp_segment_configuration table. In GP v6, the
  segment data directory is now a column in the gp_segment_configuration table.
- Read PG specific environment variables to set initial connection values. Any arguments passed
  on the command line will take precedence and override the environment.
- Added 'import logging' and changed debug "print" statement to logging calls.
  Default log level is WARNING.
'''

import sys, os, subprocess, tempfile, stat, re
from threading import Thread
from datetime import datetime
from time import localtime, strftime
import time
import getopt
import logging

try:
    from gppylib.db import dbconn
    from gppylib.gplog import *
    from gppylib.gpcatalog import *
    from gppylib.commands.unix import *
    from pygresql.pgdb import DatabaseError
    from pygresql import pg
except ImportError, e:
    sys.exit('Error: unable to import module: ' + str(e))



class schemaController():
    
    Consts_Sys_Schemas = "'gp_toolkit', 'pg_toast', 'pg_bitmapindex', 'pg_aoseg', 'pg_catalog', 'information_schema'"
    
    Consts_System_Schema_SQL = "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN (" + Consts_Sys_Schemas + ");"

    Consts_User_Schemas_SQL = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN (" + Consts_Sys_Schemas + ");"
                                             
    Consts_Table_SQL = "SELECT * FROM pg_tables WHERE schemaname='{0}';"
    
    Consts_Temp_Schema_SQL = "SELECT schema_name FROM information_schema.schemata WHERE schema_name ~ E'pg_temp_\d*';"
                              
    Consts_User_Databases_SQL = "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('gpperfmon');"
    
    
    def __init__(self):
        self.databases = []
        self.primarySegments = []
        self.masterSegment = GV.masterInstance
        GV.schemaController = self
        
        #Get all the Primary Segments
        for seg in GV.segmentDatabases:
            if seg.role == 'p':
                self.primarySegments.append(seg)
                
        cursorSchemas = self.querySegment(schemaController.Consts_User_Schemas_SQL, self.primarySegments[0])
        for schemaRow in cursorSchemas:
            print schemaRow['schema_name']
            curSQL = schemaController.Consts_Table_SQL.format(schemaRow['schema_name'])
            cursorTables = self.querySegment(curSQL, GV.masterInstance)
            for tableRow in cursorTables:
                print "\t" + tableRow['tablename']
            
                
                
    def querySegment(self,sql,seg):

        logging.info('HostName:' + seg.server +
                     ' port:' + str(seg.port) +
                     ' Content ID:' + str(seg.contentID) + ' (' + seg.dataDirectory + ')' )

        print '=================================================================='
        print 'Query: ' + sql 
        print '=================================================================='
        
        returnCurs = None

        try:
            segDB = connect(GV.username,GV.password,seg.server,seg.port,GV.database,True)
            #getDbConfig(segDB)
            #doesTableExist(segDB,GV.tableCheckName)
            try:
                curs = segDB.query(sql)   
                
                try:
                        #Deteremine if Return is recordset or number of rows. 
                    if curs.dictresult() == None:
                        #Number of row, usually from delete 
                        print curs
                    else:
                        #Recordset from query
                        returnCurs =  curs.dictresult()
                            
                except:
                    pass     
            except pg.ProgrammingError, ex:
                logging.error ('HostName:' + seg.server + ' port:' + str(seg.port) +
                               ' Content ID:' + str(seg.contentID) +
                               ' (' + seg.dataDirectory + ')' )
                print ex
            except pg.InternalError, ex:
                logging.error ('HostName:' + seg.server + ' port:' + str(seg.port) +
                               ' Content ID:' + str(seg.contentID) +
                               ' (' + seg.dataDirectory + ')' )
                print ex
            finally:
                pass
        
        
        except Exception, ex:
            logging.error ('HostName:' + seg.server + ' port:' + str(seg.port) +
                           ' Content ID:' + str(seg.contentID) +
                           ' (' + seg.dataDirectory + ')' )
            print ex
        finally:
            pass
        
        try:
            segDB.close()
        except:
            pass   
        
        return returnCurs
           
    def addDatabase(self,dbName, dbType):
        self.databases.append(schemaDatabase(dbName,dbType))
    
    def processSegment(self, connection, segment):
        return True #placeholder
    
    def processSchema(self):
        schemaSQL = schemaController.Consts_Schema_SQL.format("public")

        return True
        

# Database Types
#    User = 0
#    System = 1
#    GPDBSystem = 2

class schemaDatabase():
    def __init__(self, dbName):
        self.databaseName = dbName
        #self.databaseType = dbType
        self.tables = []
        
    def containsTable(self, tblFullName):
        for tbl in self.tables:
            if tbl.fullName == tblFullName:
                return True
        
        return False
    
    def getTable(self,tblFullName):
        for tbl in self.tables:
            if tbl.fullName == tblFullName:
                return tbl
        
        return None
    
    def addTable(self, tblName, tblSchema, tblType, contentID):
        
        fullName = tblSchema + "." + tblName
        existingRecord = self.getTable(fullName)
        
        if existingRecord != None:
            existingRecord.addSegmentWith(contentID)
        else:
            newTableRec = schemaTable(tblName,tblType,tblSchema,contentID)
            self.tables.append(newTableRec)

# Table Types
# User = 0
# System = 1
# GPDBSystem = 2
#
class schemaTable():
    def __init__(self, tblName, tblType, tblSchema,contentID):
        self.fullName = tblSchema + "." + tblName
        self.databaseName = tblName
        self.tableOID = ""
        self.tableName = ""
        self.tableSchema = tblSchema
        self.segmentsWith = []
        self.segmentsWithout = []
        self.catalogIssue = False #This means this table is not consistent across all segments & master
        self.tableType = tblType
        
        self.segmentsWith.append(contentID)

    def addSegmentWithout(self,contentID):
        self.segmentsWithout.append(contentID)
        self.catalogIssue = True

    def addSegmentWith(self,contentID):
        self.segmentsWith.append(contentID)


class segmentDatabase():
    def __init__(self):
        self.database = ""
        self.username = ""
        self.password = ""
        self.port = 0
        self.server = ""
        self.dataDirectory = ""
        self.databaseID = 0
        self.contentID = 0
        self.role = ""
        self.preferredRole = ""
        self.address = ""
        
class failedConnection():
    def __init__(self,segment,exception,exMsg):
        self.segmentDatabase = segment
        self.exception = exception
        self.exceptionMsg = exMsg

class Global():

    def __init__(self):
        self.database = ""
        self.username = ""
        self.password = ""
        self.port = 0
        self.server = ""
        
        self.tableCheckName = ""
        self.segmentDatabases = []
        self.masterInstance = None
        self.queryTarget = 0
        self.queryTargets = []
        self.segmentsSelected = []
        
        self.sqlFileName = None
        self.sqlTarget = None
        self.sqlFileContents = ""
        self.targetContentIDs = None
        
        self.failedConnections = []
        
        self.schemaController = None
        
        self.options = None
        self.args = None

        self.server_version = 0
        
    def getSegment(self,contentID):
        for seg in self.segmentDatabases:
            if seg.contentID == contentID:
                return seg

    def populateSelected(self):
        segmentsSelected = []
        
        for contentID in self.queryTargets:
            segmentsSelected.append(GV.getSegment(contentID))
            
        GV.segmentsSelected = segmentsSelected

GV = Global()


def masterMenu():
    
    printMasterSettings()

    choice = ''

    while choice != 0:
        choice = int (raw_input('Enter your choice: ') )
        if choice == 0:
            sqlMenu()
        if choice == 1:
            setValueMenu(GV.server,'Master Server',choice)
        if choice == 2:
            setValueMenu(GV.port,'Master Port',choice)
        if choice == 3:
            setValueMenu(GV.database,'Target Database',choice)
        if choice == 4:
            setValueMenu(GV.username,'User Name',choice)
        if choice == 5:
            setValueMenu(GV.password,'Password',choice)
        if choice == 6:
            reloadMasterDB()
        
        masterMenu()

def setValueMenu(value, valueName, valueID):
    valueOK = ''
    newValue = ''
    
    while valueOK != 0:
        newValue = raw_input('Enter ' + valueName + ': ')
        print '=================================================================='
        print 'Enter ' + valueName + ': ' + newValue
        print '=================================================================='
        print '2=NO 1=YES 3=BACK'
        choice = int (raw_input('Is new ' + valueName + ' Correct? ') )
        if choice == 1:
            if valueID == 1:
                GV.server = newValue
            if valueID == 2:
                GV.port = int(newValue)
            if valueID == 3:
                GV.database = newValue
            if valueID == 4:
                GV.username = newValue
            if valueID == 5:
                GV.password = newValue

            masterMenu()
        if choice == 2:
            setValueMenu(value,valueName)
        if choice == 3:
            masterMenu()


def queryMaster(sql):
    print '=================================================================='
    print 'Query: ' + sql 
    print '=================================================================='


    print 'HostName:' + GV.masterInstance.server + ' port:' + str(GV.masterInstance.port) + ' dbid:' + str(GV.masterInstance.databaseID)
    
    try:
        masterDB = connect(GV.username,GV.password, 
                           GV.masterInstance.server,GV.masterInstance.port,
                           GV.database,True)
        #getDbConfig(masterDB)
        #doesTableExist(masterDB,GV.tableCheckName)
        try:
            curs = masterDB.query(sql)   
            
            try:
                    #Deteremine if Return is recordset or number of rows. 
                if curs.dictresult() == None:
                    #Number of row, usually from delete 
                    print curs
                else:
                    #Recordset from query
                    for row in curs.dictresult():
                        print row     
            except:
                pass  
        except pg.ProgrammingError,ex:
            print ex
        except pg.InternalError, ex:
            print ex
        finally:
            pass
    
    
    except Exception, ex:
        print ex
    finally:
        pass
    

    try:
        masterDB.close()
    except:
        pass
    print '--------------------------------------------------------------'

def querySegments(sql):
    print '=================================================================='
    print 'Query: ' + sql 
    print '=================================================================='
    
    print GV.queryTargets
    for seg in GV.segmentDatabases:
       if ((GV.queryTarget != 0 and seg.contentID == GV.queryTarget) or
           (GV.queryTarget == 0 and str(seg.contentID) in GV.queryTargets) or
           (GV.queryTarget == 0 and not GV.queryTargets) ):
       
            logging.info ('HostName:' + seg.server + ' port:' + str(seg.port) +
                          ' Content ID:' + str(seg.contentID) + ' (' + seg.dataDirectory + ')' )
            
            try:
                segDB = connect(GV.username,GV.password,seg.server,seg.port,GV.database,True)
                #getDbConfig(segDB)
                #doesTableExist(segDB,GV.tableCheckName)
                try:
                    curs = segDB.query(sql)   
                    #Wrap incrase if DDL that hos no return records
                    try:
                        if curs.dictresult() == None:
                            #Number of row, usually from delete 
                            print curs
                        else:
                            #Recordset from query
                            for row in curs.dictresult():
                                print row
                    except:
                        pass
                except pg.ProgrammingError,ex:
                    print ex
                except pg.InternalError, ex:
                    print ex
                finally:
                    pass
            
            
            except Exception, ex:
                print ex
            finally:
                pass

            try:
                segDB.close()
            except:
                pass
            print '--------------------------------------------------------------'
            
def queryTargetSegments(sql,targets):
    print '=================================================================='
    print 'Query: ' + sql 
    print '=================================================================='
    
    
    for seg in targets:
      
        logging.info('HostName:' + seg.server +
                     ' port:' + str(seg.port) +
                     ' Content ID:' + str(seg.contentID) + ' (' + seg.dataDirectory + ')' )
        
        try:
            segDB = connect(GV.username,GV.password,seg.server,seg.port,GV.database,True)
            #getDbConfig(segDB)
            #doesTableExist(segDB,GV.tableCheckName)
            try:
                curs = segDB.query(sql)   
                
                try:
                        #Deteremine if Return is recordset or number of rows. 
                    if curs.dictresult() == None:
                        #Number of row, usually from delete 
                        print curs
                    else:
                        #Recordset from query
                        for row in curs.dictresult():
                            print row
                except:
                    pass     
            except pg.ProgrammingError, ex:
                logging.error ('HostName:' + seg.server + ' port:' + str(seg.port) +
                               ' Content ID:' + str(seg.contentID) +
                                             ' (' + seg.dataDirectory + ')' )
                print ex
            except pg.InternalError, ex:
                logging.error ('HostName:' + seg.server + ' port:' + str(seg.port) +
                               ' Content ID:' + str(seg.contentID) +
                                             ' (' + seg.dataDirectory + ')' )
                print ex
            finally:
                pass
        
        
        except Exception, ex:
	    logging.error ('HostName:' + seg.server + ' port:' + str(seg.port) +
			   ' Content ID:' + str(seg.contentID) +
					 ' (' + seg.dataDirectory + ')' )
            print ex
        finally:
            pass
        
        try:
            segDB.close()
        except:
            pass


def listSegments():
    print '=================================================================='
    print 'Segment List'
    print '=================================================================='
    for seg in GV.segmentDatabases:
        print ('HostName:' + seg.server + ' port:' + str(seg.port) +
               ' dbid:' + str(seg.databaseID) +
               ' Content ID:' + str(seg.contentID) + ' (' + seg.dataDirectory + ')' )

def testSegmentConnections():
    failedConnections = []
    
    for seg in GV.segmentDatabases:
        if 1 == 1:
            try:
                
                print ('Testing Content ID ' + str(seg.contentID) +
                       ' on Host ' + seg.server + ' Port ' + str(seg.port) )
                segDB = connect(GV.username,GV.password,seg.server,seg.port,GV.database,True)
                cursor = segDB.query("select version();")
                
            except Exception, ex:
                print 'Error Content ID ' + str(seg.contentID) + ' Message: ' + ex.message
                newFailure = failedConnection(segDB,ex,ex.message)
                failedConnections.append(newFailure)
            finally:   
                cursor = None
                #segDB.close()
                segDB = None
                pass
                #TODO: Stop from breaking out of script on exception
                
           
    return failedConnections


def selectQueryTarget():
    print '=================================================================='
    print 'Segment List'

    print '=================================================================='
    for seg in GV.segmentDatabases:
        print ('HostName:' + seg.server + ' port:' + str(seg.port) +
               ' dbid:' + str(seg.databaseID) +
               ' Content ID:' + str(seg.contentID) + ' (' + seg.dataDirectory + ')' )
    print '=================================================================='
    print 'Enter either single Content ID or several separated by ,'
    print 'Example Multiple: 1,2,3,4,5'
    print 'Example Single: 1'
    print '=================================================================='
    #choice = int (raw_input('Enter Target DBID: ') )
    choice = raw_input('Enter Target Content ID: ') 
    if "," in choice:
        GV.queryTargets = choice.strip().split(",")
        GV.queryTarget = 0
        GV.populateSelected()
        
    else:
        GV.queryTarget = int(choice.strip())
        GV.queryTargets = []

def printSqlMenu():
        print '=================================================================='
        print ' Segment Database Menu' 
        print '=================================================================='
        #Options
        print '[0] EXIT'
        if GV.queryTarget != 0:
            print '[1] Query Target (Content=' + str(GV.queryTarget) + ')'
        elif len(GV.queryTargets) != 0:
            print '[1] Query Targets (Content=' + ','.join(GV.queryTargets) + ')'
        else:
            print '[1] Query ALL Segments'
        print '[2] Print Segment List'
        print '[3] Target Segment(s)'
        print '[4] Clear Segment Target(s)'
        print '[5] Query Master'
        print '[6] Change Master Settings'
        print '[7] Test Connections to Primaries'
        print '[8] Load SQL file'
        
def printMasterSettings():
    
        print '=================================================================='
        print ' Master Database Settings' 
        print '=================================================================='
        #Options
        print '[0] Back'
        print '[1] Master Server Name: ' + GV.server
        print '[2] Master Port:        ' + str(GV.port)
        print '[3] Target Database:    ' + GV.database
        print '[4] User Name:          ' + GV.username
        print '[5] Password:           ' + GV.password 
        print '[6] Reload Master Configuration'
        

def printMenuHeader():
    print '=================================================================='
    print ' Utility Main Menu Header'
    print '=================================================================='
    if GV.sqlFileName != None:
        print "SQL File Loaded:{0}".format(GV.sqlFileName)

def sqlMenu():
        printMenuHeader()
        printSqlMenu()
        choice = ''

        while choice != 0:
            choice = int (raw_input('Enter your choice: ') )
            if choice == 0:
                sys.exit()
            if choice == 1:
                sqlInputMenu()
            if choice == 2:
                listSegments()
            if choice == 3:
                selectQueryTarget()
            if choice == 4:
                GV.queryTarget = 0
                GV.queryTargets = [] 
            if choice == 5:
                sqlInputMenuMaster()
            if choice == 6:
                masterMenu()
            if choice == 7:
                GV.failedConnections = testSegmentConnections()
            if choice == 8:
                menuLoadSqlFile()
            
            sqlMenu()
            
def menuLoadSqlFile():
    print '=================================================================='
    sqlFile = raw_input('Enter SQL filename: ')
    print '=================================================================='
    
    if not os.path.isfile(sqlFile):
        print "file does not exists: {0}".format(sqlFile)
        return False
    else:
        sqlText = loadSqlFile(sqlFile)
        GV.sqlFileName = sqlFile
        GV.sqlFileContents = sqlText
        return True

def sqlInputMenu():
    sqlResponse = ''
    fileChoice = ''
    sql = ''
    
    if GV.sqlFileName != None:
        print '=================================================================='
        print "SQL File Name:{0}".format(GV.sqlFileName)
        print '=================================================================='
        print  GV.sqlFileContents
        print '==================================================================' 
        while fileChoice != 0:
            fileChoice = raw_input('Use Loaded SQL from file?: ')
            if fileChoice.lower() == 'y':
                querySegments(GV.sqlFileContents)
                return
            if fileChoice.lower() == 'n':
                break
    
    
    while sqlResponse != 0:
        print '=================================================================='
        sql = raw_input('Enter SQL: ')
        print '=================================================================='
        print sql
        print '=================================================================='
        print '2=NO 1=YES 3=BACK'
        choice = int (raw_input('Is this SQL Correct? ') )
        if choice == 1:
            querySegments(sql)
            return
        if choice == 2:
            sqlInputMenu()
        if choice == 3:
            #sqlMenu()
            return
            
def sqlInputMenuMaster():
    sqlOK = ''
    sql = ''
    
    while sqlOK != 0:
        sql = raw_input('Enter SQL: ')
        print '=================================================================='
        print sql
        print '=================================================================='
        print '2=NO 1=YES 3=BACK'
        choice = int (raw_input('Is this SQL Correct? ') )
        if choice == 1:
            queryMaster(sql)
            return
        if choice == 2:
            sqlInputMenuMaster()
        if choice == 3:
            sqlMenu()
        


def parseArgs():
    from optparse import OptionParser

    # first we will check if any PG env variables are set and use those as defaults
    if 'PGHOST' in os.environ:
        GV.server = os.environ['PGHOST']
    else:
        GV.server = "mdw"

    if 'PGPORT' in os.environ:
        GV.port = int(os.environ['PGPORT'])
    else:
        GV.port = 5432

    if 'PGUSER' in os.environ:
        GV.username = os.environ['PGUSER']
    else:
        GV.username = "gpadmin"

    if 'PGPASSWORD' in os.environ:
        GV.password = os.environ['PGPASSWORD']
    else:
        GV.password = "changeme"

    if 'PGDATABASE' in os.environ:
        GV.database = os.environ['PGDATABASE']
    else:
        GV.database = GV.username

    parser = OptionParser()
    parser.add_option("-f", "--file", action="store", type="string", dest="sqlFileName",
                      help="File containing SQL to execute on target", default=None)

    parser.add_option("-d", "--database", action="store", dest="database",
                      help="Target Database Name", default = GV.database)

    parser.add_option("-t", "--target", action="store", dest = "sqlTarget",
                      help="Target Segments \n (A = ALL, M = Master Only, S = Segments Only, C = Specific Content IDs)", default=None)

    parser.add_option("-u", "--username", action="store", dest="username",
                      help="GPDB Login Username", default = GV.username)

    parser.add_option("-p", "--password", action="store", dest="password",
                      help="GPDB Login Password", default=GV.password)

    parser.add_option("-s", "--server", action="store", dest="server",
                      help="GPDB Master Server Name", default = GV.server)

    parser.add_option("-P", "--port", action="store", dest="port",
                      help="GPDB Port", default = GV.port)

    parser.add_option("-C", "--content", action="store", dest="contentIDs",
                      help="Content IDs to target", default=[])

    parser.add_option("-T", "--test", action="store_true", dest="test",
                      help="Content IDs to target", default=False)

    (GV.options, GV.args) = parser.parse_args()
    
    GV.password = GV.options.password
    GV.database = GV.options.database
    GV.username = GV.options.username
    GV.port = GV.options.port
    GV.server = GV.options.server
    GV.sqlFileName = GV.options.sqlFileName
    GV.sqlTarget = GV.options.sqlTarget
    GV.targetContentIDs = GV.options.contentIDs


    logging.info( 'host = ' + GV.server +
                  ', port = ' + str(GV.port) +
                  ', database = ' + GV.database +
                  ', sqlFileName = ' + str(GV.sqlFileName) +
                  ', sqlTarget = ' + str(GV.sqlTarget) +
                  ', targetContentIDs = ' + str(GV.targetContentIDs)
                 )

def connect(user=None, password=None, host=None, port=None, database=None, utilityMode=False):
    '''Connect to DB using parameters in GV'''
    options = utilityMode and '-c gp_session_role=utility' or None
   
    try:
        logging.debug('connecting to %s:%s %s' % (host, port, database))
        db = pg.connect(host=host, port=port, user=user,
                        passwd=password, dbname=database, opt=options)
    except pg.InternalError, ex:
        print ex
        exit(1)
    
    logging.debug('connected with %s:%s %s' % (host, port, database))     
    return db

def doesTableExist(conn,tableName):
    
    query = "select * from pg_class where relname ='" + tableName + "';";
    #query = "\\d " + tableName
    curs = conn.query(query)
    return curs
    
def getDbConfig(conn):
    
    query = "select * from gp_configuration;";
    #query = "\\d " + tableName
    curs = conn.query(query)
    print curs
    
def loadSqlFile(fileName):
    
    sqlText =""
    
    with open(fileName, "r+") as sqlFile:
        sqlText = sqlFile.read()
        
    return sqlText

def executeSqlFile(sqlFile,sqlTarget):
    
    sqlTargets = []
    sqlText = ""
    
    if not os.path.isfile(sqlFile):
        return None
    else:
        sqlText = loadSqlFile(sqlFile)
    
    if sqlTarget == "S":
        for seg in GV.segmentDatabases:
            if seg.role == 'p' and seg.contentID != '-1':
                sqlTargets.append(seg)
                
    elif sqlTarget == "M":
        sqlTargets.append(GV.masterInstance)
        print 'Master'
                
    elif sqlTarget == "A":
        for seg in GV.segmentDatabases:
            if seg.role == 'p':
                sqlTargets.append(seg)
                
        sqlTargets.append(GV.masterInstance)
    elif sqlTarget == "C":
        contentIDs = GV.targetContentIDs.split(',')
        for seg in GV.segmentDatabases:
            if seg.role == 'p' and str(seg.contentID) in contentIDs:
                sqlTargets.append(seg)
                
    else:
        print 'None'
        
    queryTargetSegments(sqlText, sqlTargets)    
    
    
def reloadMasterDB():
    db = connect(GV.username,GV.password,GV.server,GV.port,GV.database)
    if GV.server_version < 60000:
        qry ='''select b.fselocation as datadir, a.*
                from gp_segment_configuration a inner join
                     pg_filespace_entry b on a.dbid = b.fsedbid and a.role = 'p';'''
    else:
        qry ="select * from gp_segment_configuration where role = 'p' order by content;"
    
    GV.segmentDatabases = []
    
    curs = db.query(qry)
    
    for row in curs.dictresult():     
        
        server        = row['hostname']
        address       = row['address']
        port          = row['port']
        dbid          = row['dbid']
        content       = row['content']
        segDir        = row['datadir']
        role          = row['role']
        preferredRole = row['preferred_role']
        
        newSegmentDB               = segmentDatabase()
        newSegmentDB.contentID     = content
        newSegmentDB.databaseID    = dbid
        newSegmentDB.port          = port
        newSegmentDB.server        = server
        newSegmentDB.role          = role
        newSegmentDB.dataDirectory = segDir
        newSegmentDB.preferredRole = preferredRole

        if content != -1:
            GV.segmentDatabases.append(newSegmentDB)
        else:
            GV.masterInstance = newSegmentDB
        
        print ('HostName:' + server + ' port:' +
               str(port) + ' dbid:' + str(dbid) + ' (' + newSegmentDB.dataDirectory + ')' )

    db.close()
    

if __name__ == '__main__':
    
    logging.basicConfig(level=logging.WARNING)
    
    parseArgs()
    
    db = None
    curs = None
    
    try:
        db = connect(GV.username,GV.password,GV.server,GV.port,GV.database)
        logging.info('qry = "show gp_server_version_num"')
        GV.server_version = db.query('show gp_server_version_num')

        if GV.server_version < 60000:
            qry ='''select b.fselocation as datadir, a.*
                    from gp_segment_configuration a inner join
                         pg_filespace_entry b on a.dbid = b.fsedbid and a.role = 'p';'''
        else:
            qry ="select * from gp_segment_configuration where role = 'p' order by content;"
    
        curs = db.query(qry)
    
        for row in curs.dictresult():     
            
            server        = row['hostname']
            address       = row['address']
            port          = row['port']
            dbid          = row['dbid']
            content       = row['content']
            segDir        = row['datadir']
            role          = row['role']
            preferredRole = row['preferred_role']
            
            newSegmentDB               = segmentDatabase()
            newSegmentDB.contentID     = int(content)
            newSegmentDB.databaseID    = int(dbid)
            newSegmentDB.port          = int(port)
            newSegmentDB.server        = server
            newSegmentDB.role          = role
            newSegmentDB.dataDirectory = segDir
            newSegmentDB.preferredRole = preferredRole
    
            if content != -1:
                GV.segmentDatabases.append(newSegmentDB)
            else:
                GV.masterInstance = newSegmentDB
            
            logging.debug ('HostName:' + server + ' port:' +
                           str(port) + ' dbid:' +
                           str(dbid) + ' (' + newSegmentDB.dataDirectory + ')' )
            logging.debug ('HostName:' + newSegmentDB.server +
                           ' port:' + str(newSegmentDB.port) +
                           ' dbid:' + str(newSegmentDB.databaseID) +
                           ' Content ID:' + str(newSegmentDB.contentID) +
                                         ' (' + newSegmentDB.dataDirectory + ')' )
            #segDB = connect(GV.username,GV.password,server,port,GV.database,True)
            #getDbConfig(segDB)
            #doesTableExist(segDB,GV.tableCheckName)
            #segDB.close()

        #sContl = schemaController()
        
        if GV.options.test == True:
            GV.failedConnections = testSegmentConnections()
            print '=================================================================='
            print 'Failed Tests Listed Below'
            print '==================================================================' 
            print GV.failedConnections
        
        elif GV.sqlFileName != None and GV.sqlTarget != None:
            executeSqlFile(GV.sqlFileName,GV.sqlTarget)       
        else:
            sqlMenu()
            
          #  if row['content'] == -1 and row['isprimary'] != 't':
          #      continue    # skip standby master
        db.close()
    except Exception, ex:
        print ex
    #pass
