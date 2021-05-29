###############################################################
#   Date        :   25-04-2021
#   Desc        :   This class use to connect oracle DB.
#                   It has function to query database and 
#                   return table as pandas dataframe
#   Author      :   Vikas Datir
#   Modified    :   29-05-2021
#
#
###############################################################


import cx_Oracle as ora
import pandas as pd
import os
import sys
import numpy as np
import logging
import csv


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
            #setting path for oracle client files 
            sys.path.append(sys.path[0]+'\\OracleConn\\instantclient_12_2')
            self.ora_server = self.ora_server+':'+self.ora_port+'/'+self.ora_sid
            self.ora_conn = ora.connect(self.ora_user, self.ora_password, self.ora_server, encoding='UTF-8')
        except ora.Error as e:
            self.logger.error('OraConnect error : ', str(e))


    def sql_reader(self, sql_str='', file_ref=None, delimiter='|', encode='UTF-8'):
        try:
            #getting table name from sql query 
            tbl_name = sql_str.lower().partition('from')[2].upper().strip()

            #search for table
            src_table = list(pd.read_sql('SELECT TABLE_NAME FROM USER_TABLES',self.ora_conn).query("TABLE_NAME =='"+tbl_name+"'").iloc[0])

            #read data from oracle table and write to csv file
            if tbl_name == src_table[0]:
                self.tbl_df = pd.read_sql(sql_str, self.ora_conn)
                self.tbl_df = self.tbl_df.replace(to_replace=np.nan, value='')

                #writing dataframe to csv file 
                #self.tbl_df.to_csv(file_ref, sep=delimiter, index=False, encoding=encode, line_terminator='\n',quoting=csv.QUOTE_NONNUMERIC)
                self.tbl_df.to_csv(file_ref, sep=delimiter, index=False, encoding=encode, line_terminator='\n',quoting=csv.QUOTE_NONNUMERIC)
            else:
                self.logger.error('Table or view does not exists')
                raise 
        except Exception as e :
            self.logger.error('OraConnet Error : ', str(e))



    def sql_tab_caller(self, table_list=None, file_loc=None, sep='|', ):
        try:            
            #setting table list needs to be extracted 
            self.tables = None
            self.bkt_path= file_loc.bucket_dir_path
            self.tbl_dir = {}
            if len(table_list)==0:
                #extract all tables by creating list of all table 
                self.tables=list(pd.read_sql('SELECT TABLE_NAME FROM USER_TABLES',self.ora_conn)['TABLE_NAME'])
            else:
                self.tables=table_list
                
            #passing table names to extact data 
            for table in self.tables:
                file_loc.create_dir(self.bkt_path,table)
                with open(file_loc.bucket_dir_path+'\\'+table+'.txt', 'a+', encoding='utf-8') as fptr:
                    self.sql_reader('SELECT * FROM '+table ,fptr)
                #print(file_loc.bucket_dir_path)
                self.tbl_dir[table+'.zip']=file_loc.bucket_dir_path
                
        except Exception as e:
            self.logger.error('OraConnet Error : ', str(e))

            
    def sql_close(self):
        try:
            self.ora_conn.close()
        except ora.Error as e:
            self.logger.error('OraConnet Error : ', str(e))

def main():
    #testing
    obj=ORAConnect(username='hr', password='080522', server='localhost', port='1521', sid='xe', schema='hr' )
    obj.sql_tab_caller()


if '__name__' == '__main()__':
    try:
        main()
    except Exception as e:
        print(str(e))

