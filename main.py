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
import os 
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
            self.logger.error('No internet connection.')
        except FileNotFoundError as e:
            self.logger.error('Josn file not found')
        except Exception as e :
            self.logger.error('Error in caller ' +str(e))
    
    def orcl_caller(self):
        try:
            #creating oracle object 
            self.ora_obj = ORAConnect(server=self.oracle_spec['server'],
                username=self.oracle_spec['username'],
                password=self.oracle_spec['password'],
                port=self.oracle_spec['port'],                                    
                sid=self.oracle_spec['sid'],
                schema=self.oracle_spec['schema'])
        except Exception as e :
            self.logger.error('Error in orcl_caller method ' +str(e))

    def aws_s3_caller(self):
        try:
            #creating aws s3 connection object            
            self.s3_obj = S3Connect(access_key=self.asw_s3_spec['access_key'], secret_key=self.asw_s3_spec['secret_key'], region=self.asw_s3_spec['region'])
        except Exception as e :
            self.logger.error('Error in aws_s3_caller method ' +str(e))
        
    



#Data_Transfer('D:\\Orcl-To-S3\\control.json').orcl_caller()
Data_Transfer('D:\\Orcl-To-S3\\control.json').aws_s3_caller()


