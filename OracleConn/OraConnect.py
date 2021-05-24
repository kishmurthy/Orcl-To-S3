###############################################################
#   Date        :   25-04-2021
#   Desc        :   This class use to connect oracle DB.
#                   It has function to query database and 
#                   return table as pandas dataframe
#   Author      :   Vikas Datir
#   Modified    :   01-05-2021
#
#
###############################################################


import cx_Oracle as ora
import pandas as pd
import os
import sys
import numpy as np
import logging

class ORAConnect:    
    def __init__(self,**para):
        self.ora_server = para['server']
        self.ora_user =  para['username']
        self.ora_password = para['password']
        self.ora_port = para['port']
        self.ora_sid = para['sid']
        self.ora_schema = para['schema']
        self.logger = logging.getLogger(__name__)
        self.logger.info("Oracle service started")
        try:
            sys.path.append(sys.path[0]+'\\OracleConn\\instantclient_12_2')
            self.ora_server = self.ora_server+':'+self.ora_port+'/'+self.ora_sid
            self.ora_conn = ora.connect(self.ora_user, self.ora_password, self.ora_server, encoding='UTF-8')
        except ora.Error as e:
            self.logger.error('OraConnect error : ', str(e))

    def sql_reader(self, sql_str):
        try:
            #getting table name from sql query 
            tbl_name = sql_str.lower().partition('from')[2].upper().strip()

            #search for table
            src_table = list(pd.read_sql('SELECT TABLE_NAME FROM USER_TABLES',self.ora_conn).query("TABLE_NAME =='"+tbl_name+"'").iloc[0])
            
            if tbl_name == src_table[0]:
                self.tbl_df = pd.read_sql(sql_str, self.ora_conn)
                self.tbl_df = self.tbl_df.replace(to_replace=np.nan, value='')
                return self.tbl_df
            else:
                self.logger.error('Table or view does not exists')
                raise 
        except Exception as e :
            self.ora_conn.close()

            
    def sql_close(self):
        try:
            self.ora_conn.close()
        except ora.Error as e:
            self.logger.error('OraConnet Error : ', str(e))

def main():
    #testing
    obj=ORAConnect(user='hr', password='080522', server='localhost:1521/xe')
    obj.SqlClose()

if '__name__' == '__main()__':
    try:
        main()
    except Exception as e:
        print(str(e))

