#!/usr/bin/env python

import MySQLdb
from MySQLdb.cursors import DictCursor
from mysql_creds import *

def _cxn(**kwargs):
    return MySQLdb.connect(host=DB_HOSTNAME, user=DB_USERNAME, passwd=DB_PASSWORD, db=DB_NAME, **kwargs)

def default_cursor(**kwargs):
    return _cxn().cursor(**kwargs)

def dict_cursor(**kwargs):
    return _cxn(cursorclass=DictCursor, **kwargs).cursor()

if '__main__' == __name__:
    cur = dict_cursor()
    cur.execute('SELECT VERSION()')
    print 'MySQL Database Version: %s' % cur.fetchall()[0]['VERSION()']