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



from OracleConn.OraConnect import ORAConnect as ora 
import os 
import json as js
import logging 




class Transfer:
    def __init__(self, json_loc=''):
        self.json_file = json_loc
        try:
            with open(self.json_file) as fptr:
                spec =  js.load(fptr)
            self.log_spec = spec['logger']
            self.oracle_spec = spec['oracle']
            self.file_spec = spec['file']
            self.asw_s3_spec = spec['aws_s3']
            
            logging.basicConfig(level=logging.DEBUG, filename=self.log_spec['transfer'],format='%(asctime)s  %(name)s  %(levelname)s : %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
            self.logger = logging.getLogger(__name__)
            self.logger.debug("Transfer service started")

        except FileNotFoundError as e:
            self.logger.error('Josn file not found')
        except Exception as e :
            self.logger.error('Error in caller ' +str(e))
    
    def orcl_call(self):
        try:
            ora(server=self.oracle_spec['server'],username=self.oracle_spec['username'],password=self.oracle_spec['password'],port=self.oracle_spec['port'],sid=self.oracle_spec['sid'],schema=self.oracle_spec['schema'])
        except Exception as e :
            self.logger.error('Error in orcl_caller function ' +str(e))





Transfer('D:\\Orcl-To-S3\\control.json').orcl_call()
