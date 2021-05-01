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

class ORAConnect:
    __ora_server = None
    __ora_user = None
    __ora_password = None
    __ora_schema = None
    
    def __init__(self, **para):
        __ora_server = para['server']
        __ora_user =  para['user']
        __ora_password = para['password']
        try:
            sys.path.append(sys.path[0]+'\\OracleConn\\instantclient_12_2')
            self.ora_conn = ora.connect(__ora_user, __ora_password, __ora_server, encoding='UTF-8')
        except ora.Error as e:
            sys.stdout.write('OraConnect error : ', str(e))

    def SqlReader(self, sql_str):
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
                sys.stdout.write('Table or view does not exists')
                raise 
        except Exception as e :
            self.ora_conn.close()
            
    def SqlClose(self):
        try:
            self.ora_conn.close()
        except ora.Error as e:
            sys.stdout.write('OraConnet Error : ', str(e))

def main():
    #testing
    obj=ORAConnect(user='hr', password='080522', server='localhost:1521/xe')
    obj.SqlClose()

if '__name__' == '__main()__':
    try:
        main()
    except Exception as e:
        print(str(e))

