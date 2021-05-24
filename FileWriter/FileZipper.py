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

import logging
import os 
import shutil
import sys
import pandas as pd 

class FileZipper:
    def __init__(self, stage_loc='D:\\Orcl-To-S3'):
        try:
            self.logger = logging.getLogger(__name__)
            if stage_loc!=None:
                self.stage_loc = stage_loc
            else:
                raise Exception('Stage location not specified. Please check contol Josn file')
        except Exception as e:
            self.logger.error("#"*10,str(e))
            print('error')
            
    def create_dir(self, dir_name=None):
        try:
            self.bcuket_dir = dir_name
            self.bucket_dir_path = os.path.join(self.stage_loc, self.bcuket_dir)
            os.mkdir(self.bucket_dir_path)
        except FileExistsError as e:
            self.logger.error("#"*10,str(e))

    def file_writer(self, table_df=None, delimiter='|', file_ref=None):
        try:
            if table_df !=None and file_ref!=None:
                self.table_df = table_df.to_csv(file_ref, sep="|", index=False, encoding='utf-8', line_terminator='\n',quoting=csv.QUOTE_NONNUMERIC)
            else:
                raise Exception('Error in in file writer method')
        except Exception as e :
            self.logger.error("#"*10,str(e))

    def zipper(self,table_dir_path=None):
        try:
            if os.path.is_dir(self.table_dir_path): 
                self.table_dir_path = table_dir_path
                shutil.make_archive(self.table_dir_path, 'zip',self.table_dir_path)
            else:
                raise
        except FileNotFoundError as e :
            self.logger.error("#"*10,str(e))        
        except shutil.Error as e :
            self.logger.error("#"*10,str(e))
        

o=FileZipper()
o.create_dir('Orcl-data1')
o.zipper('Orcl-data1')


