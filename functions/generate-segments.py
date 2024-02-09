# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import random
import os
import boto3
import json

def lambda_handler(event, context):
    id = event['executionId'].split(':')[-1]
    
    segments = list(range(0, int(os.environ['TotalSegments'])))
    random.shuffle(segments)
    boto3.client("s3").put_object(Bucket=os.environ['BucketName'], Key=f"stepfunctionconfig-{id}.json", Body=str.encode(json.dumps(segments)))
    return { 'bucket' : os.environ['BucketName'], 'key' : f"stepfunctionconfig-{id}.json" }