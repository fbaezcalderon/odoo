# -*- coding: utf-8 -*-


import pandas as pd
import configparser
import os
import sys

CONNECTORS = []
try:
    import psycopg2
    CONNECTORS.append(('postgres', 'POSTGRES'))
except:
    print('PSYCOPG2 libraries not available. Please install.')

try:
    import cx_Oracle
    CONNECTORS.append(('cx_Oracle', 'Oracle'))
except:
    print('CX_ORACLE libraries not available. Please install.')

class DataBaseConnector:
    
    def __init__(self, source=None,use_sid=None):
        conf = configparser.ConfigParser()                   
        conf.read('config.ini')
        self.use_sid=False
        
        self.host=conf[source]['host']
        self.port=conf[source]['port']
        self.user=conf[source]['user']
        self.service_name = conf[source]['database_name']
        self.password = conf[source]['password']
        self.connector = conf[source]['connector']
        
    
    def conn_open(self):
        """The connection is open here."""

        # Try to connect
        if self.connector == 'cx_Oracle':
            os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.UTF8'
            
            if self.use_sid:
                dsnStr = cx_Oracle.makedsn(self.host, self.port, self.sid)
                conn = cx_Oracle.connect(user=self.user,password=self.password,dsn=dsnStr)
            else:
                #----- print(self.host+' '+str(self.port)+' '+self.service_name)
                dsnStr = cx_Oracle.makedsn(host=self.host, port=self.port, service_name=self.service_name)
                conn = cx_Oracle.connect(user=self.user,password=self.password,dsn=dsnStr)
       
        elif self.connector=='postgresql':
            conn = psycopg2.connect(user = self.user,
                                  password = self.password,
                                  host = self.host,
                                  port = self.port,
                                  database = self.service_name)    
        
        return conn
   
    def execute_insert(self, sqlquery, sqlparams=None, metadata=False, context=None):
        conn = self.conn_open()
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sqlquery, sqlparams)
        
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()        
    
    def execute(self, sqlquery, sqlparams=None, metadata=False, dataframe=False):
        """Executes SQL and returns a list of rows.

            "sqlparams" can be a dict of values, that can be referenced in
            the SQL statement using "%(key)s" or, in the case of Oracle,
            ":key".
            Example:
                sqlquery = "select * from mytable where city = %(city)s and
                            date > %(dt)s"
                params   = {'city': 'Lisbon',
                            'dt': datetime.datetime(2000, 12, 31)}

            If metadata=True, it will instead return a dict containing the
            rows list and the columns list, in the format:
                { 'cols': [ 'col_a', 'col_b', ...]
                , 'rows': [ (a0, b0, ...), (a1, b1, ...), ...] }
        """
        # data = self.browse(cr, uid, ids)
        rows, cols = list(), list()
        
        
        conn = self.conn_open()
        if self.connector in ["sqlite", "mysql", "mssql"]:
                # using sqlalchemy
            cur = conn.execute(sqlquery, sqlparams)
            if metadata:
                cols = cur.keys()
            rows = [r for r in cur]
        else:
            # using other db connectors
            cur = conn.cursor()
            if dataframe:
                df_result = pd.read_sql_query(sql=sqlquery, con=conn, params=sqlparams)
                
                df_result.columns = map(str.lower, df_result.columns)
                conn.close()    
                return df_result
            
            cur.execute(sqlquery, sqlparams)
            if metadata:
                cols = [d[0] for d in cur.description]
            
            rows = cur.fetchall()
        conn.close()
        if metadata:
            return{'cols': cols, 'rows': rows}
        else:
            if dataframe:
                return df_result
            else:
                return rows

   
    def connection_test(self):
        """Test of connection."""
               
        for obj in self:
            conn = False
            try:
                conn = self.conn_open()
            except Exception as e:
                sys.stdout.write("Error while connecting to the database: "+e)
            finally:
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
        
    def get_dataframe_from_postgres(self,sqlquery,sqlparams):
        conn=self.conn_open()
        df_result = pd.read_sql_query(sql=sqlquery, con=conn, params=sqlparams)
        df_result.columns = map(str.lower, df_result.columns)
        conn.close()
        return df_result
        
        
        