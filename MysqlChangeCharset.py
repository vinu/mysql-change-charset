'''
Created on 23-Mar-2011

@author: vinu

Simple script to change the charset and collation for the text fields in the db on the fly
'''

import logging
import logging.handlers
import MySQLdb
import MySQLdb.cursors


LOGGER_NAME = "mysqlchange"

LOG_LEVEL = logging.DEBUG

'''
DB configs
'''
DBHOST='127.0.0.1'
DBUSER='root'
DBPASS=''
DBDB='demo'

'''
Set the charset and collation
'''
CHARSET = 'utf8'
COLLATION = 'utf8_general_ci'


if __name__ == '__main__':
    log = logging.getLogger(LOGGER_NAME)    
    log.setLevel(LOG_LEVEL)
    
    import os
    scriptname =  os.path.realpath(__file__);
    curdir =  os.path.dirname(scriptname)
    logfile = os.path.join(curdir,"mysqlchangecharset.log");
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    
    rlh = logging.handlers.RotatingFileHandler(logfile,maxBytes=20 * 1024, backupCount=5)
    rlh.setFormatter(formatter)
    log.addHandler(ch)
    #log.addHandler(rlh)
    conn = MySQLdb.connect(host = DBHOST,
                           user = DBUSER,
                           passwd = DBPASS,
                           db = DBDB,
                           cursorclass=MySQLdb.cursors.DictCursor)
    cursor = conn.cursor()
    
    cursor.execute ('show tables')
    result_set = cursor.fetchall ()
    cursor.close();
    
    for t in result_set:
        table =  t['Tables_in_%s' % (DBDB)]
        cursor = conn.cursor()
        atq = 'alter table %s CHARSET=%s COLLATE=%s' %(table,CHARSET,COLLATION)
        cursor.execute (atq)
        cursor.close();        
        log.info("Altered table %s" %(table))
                
        q = 'show full fields from %s' %(table)
        cursor = conn.cursor()
        cursor.execute (q)
        fieldset = cursor.fetchall ()
        cursor.close();
        
        for f in fieldset:
            fname =  f['Field'] 
            ftype =  f['Type']
            fcollation = f['Collation']                        
            if ftype.startswith('varchar') or fcollation is not None:
                atfq = "ALTER TABLE %s MODIFY COLUMN %s.%s %s CHARACTER SET %s COLLATE %s" %(table,table,fname,ftype,CHARSET,COLLATION)
                try:
                    cursor = conn.cursor()
                    cursor.execute (atfq)
                    cursor.close();
                    log.info(">> Changed field %s for table %s" %(fname,table))
                except:
                    log.error('Error executing %s' %(atfq))

    cursor.close();
    
    conn.close()
    
    pass