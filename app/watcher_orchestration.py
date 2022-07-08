from boto3 import session
from botocore.client import Config
import yaml
from sqlalchemy import create_engine
import io
import os

import pandas as pd
import requests

def create_client(source):
    if source['type'] == 's3':
        s3_session = session.Session()
        return s3_session.client(
            's3',
            region_name=source['region_name'],
            endpoint_url=source['endpoint_url'],
            aws_access_key_id=source['access_key_id'],
            aws_secret_access_key=source['secret_access_key']
        )
    elif source['type'] == 'postgresql':
        return create_engine("postgresql://%s:%s@%s:%s/%s" % (
            source['username'], 
            source['password'], 
            source['host'], 
            source['port'],
            source['database']
        ))
    else:
        print('No supported source type found')
        return False

os.mkdir('tmp')

config = {}
with open('./config.yaml') as f:
    config = yaml.full_load(f)

for name, w in config['watchers'].items():
    func = getattr(__import__("scripts." + w['function'], fromlist=['run']), 'run')

    if w['source'] not in config['sources']:
        print("Source for %s does not exist!" % name)
        continue

    def task(config, w):
        main_source = config['sources'][w['source']]

        clients = []

        # If the watcher is mounted onto an S3 bucket
        if main_source['type'] == 's3':
            s3_client = create_client(main_source)

            clients.append(s3_client)

            # Add any additional sources to clients object
            for r in w['additional_sources']:
                clients.append(create_client(config['sources'][r]))

            # Loop over each file in the bucket on the specified path
            for obj in s3_client.list_objects(Bucket=w['bucket'], Prefix=w['path'])['Contents']:
                fname = obj['Key'].split('/')[-1:][0]
                s3_client.download_file(Bucket=w['bucket'], Key=obj['Key'], Filename="tmp/" + fname)

                task_result = func(clients, "tmp/" + fname) if 'args' not in w else func(clients, "tmp/" + fname, **w['args'])
                if task_result and 'archive_bucket' in w:
                    # execution successfull and watcher is configured to archive passed objects
                    print("Archiving %s" % f['Key'])
                    s3_client.copy_object(Bucket=w['archive_bucket'], CopySource="%s/%s" % (w['bucket'], obj['Key']), Key=obj['Key'])
                    s3_client.delete_object(Bucket=w['bucket'], Key=f['Key'])

    task(config, w)


    


