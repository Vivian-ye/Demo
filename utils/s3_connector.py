import boto3

class S3Connector():
    def __init__(self, bucket):
        self.bucket = bucket
        self.s3_client = boto3.client('s3')

    def upload_to_s3(self, table_name, object_name):
        self.s3_client.upload_file(f"{table_name}.csv", self.bucket, f"{object_name}/{table_name}.csv")