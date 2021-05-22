###############################################################
#   Date        :   15-05-2021
#   Desc        :   This class use to connect was s3 service.
#                   It has function to create buckets and 
#   Author      :   Vikas Datir
#   Modified    :   21-05-2021
#
#
#
#
#
###############################################################


import logging
import boto3
from botocore.exceptions import ClientError

class S3Connect:
    def __init__(self, access_key=None, secret_key=None, region=None):
        try:
            self.logger = logging.getLogger(__name__)
            
            #checking user details 
            if access_key!=None and secret_key!=None and region!=None:
                self.access_key=access_key
                self.secret_key=secret_key
                self.region=region
                self.s3_conn = boto3.client('s3',
                                            aws_access_key_id = access_key,
                                            aws_secret_access_key = secret_key,
                                            region_name = region)
                self.logger.info("Aws S3 connected")
            else:
                self.logger.error("Please check access_key, secret_key and region details")
        except ClientError as e :
            self.logger.error("Aws S3 Connection error : "+str(e))


    def create_s3_bucket(self, bucket_name):
        try:
            self.bucket_name=bucket_name
            location = {'LocationConstraint': self.region}
            self.s3_conn.create_bucket(Bucket=self.bucket_name,CreateBucketConfiguration=location)
        except ClientError as e :
            print(str(e) +'logg')

    def file_transfer(self, bucket_name='orcl-stg', file_name='D:\\EMP.csv', object_name=None):
        try:
            if self.s3_conn.head_bucket(Bucket=bucket_name) !=None:
                print('Bucket is presnet')
            else:
                raise 
            if object_name is None:
                object_name = "EMP.csv"
                self.s3_conn.upload_file(file_name, bucket_name, object_name)     
        except ClientError as e:
            logging.error(str(e))


#S3Connect().file_transfer()
