import boto3


class Boto3ConnectorLogs(object):

    def __init__(self):
        self.conn = boto3.resource('s3',
                                   endpoint_url='http://localhost:9000',
                                   aws_access_key='minio',
                                   aws_secret_access_key='minio123')

        self.bucket = 'logs'

    def get_all_files(self):
        files = []
        bucket = self.conn.Bucket(self.bucket)
        for file_obj in bucket.objects.all():
            files.append(file_obj.key)
        return files

    def upload(self, file_name, upload_from='/tmp/'):
        self.conn.meta.client.upload_file(file_name, self.bucket, upload_from + file_name)

    def download(self, file_name, download_to='/tmp/'):
        self.conn.meta.client.download_file(self.bucket, file_name, download_to + file_name)
