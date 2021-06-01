###############################################################
#   Date        :   15-05-2021
#   Desc        :   This class use to connect aws s3 service.
#   Use         :   To create S3 bucket and transfer files from
#                   local machine.
#   Author      :   Vikas Datir
#   Modified    :   01-06-2021
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
                self.logger.info("Connected to the AWS S3")
            else:
                self.logger.error("#"*10,"Please check access_key, secret_key and region details")
        except ClientError as e :
            self.logger.error("#"*10,"Aws S3 Connection error : "+str(e))


    def create_s3_bucket(self, bucket_name):
        try:
            self.bucket_name=bucket_name
            location = {'LocationConstraint': self.region}
            if self.s3_conn.create_bucket(Bucket=self.bucket_name,CreateBucketConfiguration=location):
                self.logger.info("AWS %s bucket has been created in region %s",self.bucket_name,self.region)
            else:
                raise
        except ClientError as e :
            self.logger.error("#"*10, "Aws S3 bucket creation error : "+str(e))

    def file_transfer(self, file_name=None, object_name=None):
        try:
            if self.s3_conn.head_bucket(Bucket=self.bucket_name) !=None:
                self.file_name = file_name
                self.object_name = object_name
                self.s3_conn.upload_file(self.file_name, self.bucket_name, self.object_name)
                self.logger.info(self.file_name+ " has been transferred")
            else:
                raise
        except ClientError as e:
            self.logging.error("#"*10,"Aws S3 file transfer error : "+str(e))


