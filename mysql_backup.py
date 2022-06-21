import os
import time
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from loguru import logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import re
import subprocess
from dotenv import load_dotenv

load_dotenv()


logger.add("/var/log/logger/script_backup.log", format="{time} - {level} - {message}")


class Dumper():
    
    def __init__(self) -> None:
        self.s3_access_key = os.environ['S3_ACCESS_KEY']
        self.s3_secret_key = os.environ['S3_SECRET_KEY']
        self.s3_bucket_name = os.environ['S3_BUCKET_NAME']
        self.slack_key  = os.environ['SLACK_KEY']
        self.client = WebClient(token=self.slack_key)
        self.databases = os.environ['DATABASES']
        self.backup_dir = os.environ['S3_BUCKET_NAME']
        self.container_name = os.environ['CONTAINER_NAME']
        self.mysql_username = os.environ['MYSQL_USERNAME']
        self.mysql_password = os.environ['MYSQL_PASSWORD']
        self.channel = os.environ['CHANNEL']
    
    def dumping(self) -> int:
        backup_time = time.strftime('%Y-%m-%d_%H-%M')
        if os.path.isdir(self.backup_dir):
            for database in self.databases.split(' '):
                mysqldump_command = f"docker exec {self.container_name} \
                                    /usr/bin/mysqldump -u {self.mysql_username} -p{self.mysql_password} \
                                    {database} > {self.backup_dir}{backup_time}-{database}.sql"
                archiving_command = f"/bin/gzip {self.backup_dir}{backup_time}-{database}.sql"
                dump = subprocess.run(mysqldump_command, shell=True)
                if dump.returncode == 0:
                    do_archive = subprocess.run(archiving_command, shell=True)
                    return do_archive.returncode
                raise RuntimeError('Failed to execute command')
        raise OSError('The directory does not exist')
    
    def slack_alert(self) -> None:
        try:
            response = self.client.chat_postMessage(channel=self.channel, text='Backup of zomro DB was created successfully')
        except SlackApiError as e:
            assert e.response['ok'] is False
            assert e.response['error']
            logger.error(f"Got an error: {e.response['error']}")
    
    def upload_file(self, file_name: str, bucket: str, object_name: str = None) -> bool:
        if object_name is None:
            object_name = file_name

        s3_client = boto3.client('s3', aws_access_key_id=self.s3_access_key, aws_secret_access_key=self.s3_secret_key)
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logger.error(e)
            return False
        return True

    def main(self) -> None:
        try:
            if self.dumping() == 0:
                for file in Path(self.backup_dir).iterdir():
                    file_name = str(file).split('-')[-1]
                    if re.match('%s%s02-00-%s' % (self.backup_dir, time.strftime('%Y-%m-%d_'), file_name),  str(file)):
                        self.upload_file(str(file), self.s3_bucket_name, object_name=None)
                        self.slack_alert()
                        logger.success('Backup was created')
        except Exception as err:
            self.client.chat_postMessage(channel=self.channel, text='Error: %s' % err)

if __name__ == '__main__':
    dumper = Dumper()
    dumper.main()
