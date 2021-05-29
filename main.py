######################################################################
#
#
#
#
#
#
#
#
#
######################################################################



from OracleConn.OraConnect import ORAConnect
from AwsS3Conn.S3Connect import S3Connect
from FileWriter.FileZipper import FileZipper
from datetime import datetime
import os
import sys
import json as js
import requests as req
import logging


class Data_Transfer:
    def __init__(self, json_loc=''):
        self.json_file = json_loc
        
        try:

            #reading json file 
            with open(self.json_file) as fptr:
                self.json_spec =  js.load(fptr)
                
            #setting control variables
            self.file_spec = self.json_spec['file']
            self.asw_s3_spec = self.json_spec['aws_user']
            self.log_spec = self.json_spec['logger']
            self.oracle_spec = self.json_spec['oracle']

            #setting logger 
            logging.basicConfig(level=logging.DEBUG,
                                filename=self.log_spec['transfer'],
                                format='%(asctime)s  %(name)s  %(levelname)s : %(message)s',
                                datefmt="%Y-%m-%d %H:%M:%S")
            self.logger = logging.getLogger(__name__)
            self.logger.info("Data Transfer service started")

            #testing internet connection
            if req.get("http://www.google.com", timeout=10)!=None:
                self.logger.info("Connected to the Internet")
         
        except (req.ConnectionError, req.Timeout) as e:
            self.logger.error("#"*10,'No internet connection.')
            sys.exit()
        except FileNotFoundError as e:
            self.logger.error("#"*10,'Josn file not found')
        except Exception as e :
            self.logger.error("#"*10,'Error in Data_Transfer init' +str(e))
    
    def orcl_init(self):
        try:
            #creating oracle object 
            self.ora_obj = ORAConnect(server=self.oracle_spec['server'],
                username=self.oracle_spec['username'],
                password=self.oracle_spec['password'],
                port=self.oracle_spec['port'],                                    
                sid=self.oracle_spec['sid'],
                schema=self.oracle_spec['schema'])
        except Exception as e :
            self.logger.error("#"*10,'Error in orcl_init method ' +str(e))

    def aws_s3_init(self):
        try:
            #creating aws s3 connection object            
            self.s3_obj = S3Connect(access_key=self.asw_s3_spec['access_key'],
                                    secret_key=self.asw_s3_spec['secret_key'],
                                    region=self.asw_s3_spec['region'])
            #creating aws-s3 bucket
            self.bucket_name = self.asw_s3_spec['bucket_name_prefix']+'-'+datetime.now().strftime("%Y-%m-%d").lower().strip()
            self.s3_obj.create_s3_bucket(self.bucket_name)
            
        except Exception as e :
            self.logger.error("#"*10,'Error in aws_s3_init method ' +str(e))

    def file_loc_init(self):
        try:
            #setting file location object 
            self.f_loc_obj= FileZipper(self.file_spec['file_location'])
            self.logger.info("File location object has created at : "+self.f_loc_obj.stage_loc)
            
            #creating batch folder with date
            self.f_loc_obj.create_dir(self.file_spec['file_location'],'Orcl_to_S3_batch_'+datetime.now().strftime("%Y-%m-%d"))
            self.logger.info("Batch folder has created at : "+self.f_loc_obj.bucket_dir_path)

        except Exception as e:
            self.logger.error("#"*10,'Error in file_loc_init method ' +str(e))

    def orcl_extract(self):
        try:
            #extracting data 
            self.ora_obj.sql_tab_caller(self.oracle_spec['tables'],self.f_loc_obj)

            #zipping table folders
            for file, path in self.ora_obj.tbl_dir.items():
                self.f_loc_obj.zipper(path)
                

            #transfer file to S3
            for file, path in self.ora_obj.tbl_dir.items():
                self.s3_obj.file_transfer(path+'.zip',file)
            
             
        except Exception as e:
            self.logger.error("#"*10,'Error in aws_s3_init method ' +str(e))
            
    
def main():
    o=Data_Transfer('D:\\Orcl-To-S3\\control.json')
    o.orcl_init()
    o.file_loc_init()
    o.aws_s3_init()
    o.orcl_extract()
    
    
if __name__ == '__main__':
    main()
