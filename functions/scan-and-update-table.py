# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import os
import boto3
import time

####################################################################
#
# YOU MUST UPDATE THIS METHOD
#
# source_table and destination_table are both DynamoDB Table resources. See
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
# if you are scanning and updating the same table, use either to write as they both point at the same table.
# if you want to use put or delete in batches and want a convenience class, see the source_batcher and destination_batcher
# below.
#
# source_batcher and destination_batcher are both batch_writerers. See 
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/batch_writer.html
# source_batcher is a writer pointing at the table the scan was performed against
# destination_batcher is a writer pointing at the table to receive the updated item
# if you are scanning and updating the same table, use either writer as they both point at the same table
#
# item is a dict of your DynamoDB item to be processed
#
# update this method to perform the processing you desire on the item
# Ideally ensure any modifications are idempotent to enable re-processing if necessary
#
####################################################################
def process_item(source_table, destination_table, source_batcher, destination_batcher, item):
  
    raise Exception('You shoud replace this method code with your item modification. Some example modifications can be found below.')
    
    # EXAMPLE - add extra attribute(s) to the item
    # if 'processed_at' not in item:
    #     destination_table.update_item(
    #         Key={ 'pk': item['pk'], 'sk' : item['sk'] },
    #         UpdateExpression='SET processed_at = :processed_at',
    #         ExpressionAttributeValues={ ':processed_at': int(time.time()) }
    #     )
    
    # EXAMPLE - delete the item if a condition holds
    # if 'delete_at' in item and item['delete_at'] > int(time.time()):
    #     destination_batcher.delete_item(Key={'pk' : item['pk'], 'sk' : item['sk']})
      
    # EXAMPLE - copy the item from source_table to destination_table and mark it as migrated
    # if 'migrated' not in item:
    #     destination_batcher.put_item(Item=item)
    #     source_table.update_item(
    #         Key={ 'pk': item['pk'], 'sk' : item['sk'] },
    #         UpdateExpression='SET migrated = :migrated',
    #         ExpressionAttributeValues={ ':migrated': 1 }
    #     )


####################################################################
#
#  It is unlikely you will need to modify anything below this point
#
####################################################################
def rate_limit(rate_limiter):
    if rate_limiter['limit'] >= 0:
        if int(time.monotonic()) > rate_limiter['second']:
            rate_limiter['second'] = int(time.monotonic())
            rate_limiter['count'] = 1
            return
    
        rate_limiter['count'] += 1
        if rate_limiter['count'] < rate_limiter['limit']:
            return
          
        sleep = (rate_limiter['second'] + 1) - time.monotonic() 
        if sleep > 0:
            time.sleep(sleep) 

def log_progress(segments, items, last_message):
    if time.monotonic() - last_message > 60:
        print(f"Processing segments {segments}. {items} items processed so far")
        return time.monotonic()
  
    return last_message

def lambda_handler(event, context):
    segments = event['Items'] 
    total_segments = int(os.environ['TotalSegments'])
    consistent_read = ('true' == os.environ['ConsistentRead'])
    page_size = int(os.environ['PageSize'])
    
    rate_limiter = { 'limit' : int(os.environ['RateLimit']), 'second' : 0, 'count' : 0 }
    next_tokens = dict.fromkeys(segments, '')
    items = 0
    started_at = time.monotonic()
    last_message = 0
    
    dynamo = boto3.resource('dynamodb')
    source_table = dynamo.Table(os.environ['SourceTableName'])
    destination_table = dynamo.Table(os.environ['DestinationTableName'])
    
    print(f"Process segments {segments}")
    
    with source_table.batch_writer() as source_batch:
        with destination_table.batch_writer() as destination_batch:
            while len(next_tokens) > 0:  
                for segment in segments:      
                    if segment in next_tokens:
                        last_message = log_progress(segments, items, last_message)
                        
                        kwargs={
                            'Segment' : segment,
                            'TotalSegments' : total_segments,
                            'Limit' : page_size,
                            'ConsistentRead' : consistent_read
                        }
                        
                        if len(next_tokens[segment]) > 0:
                            kwargs['ExclusiveStartKey'] = next_tokens[segment]  
                        
                        response = source_table.scan(**kwargs)
                        
                        for item in response['Items']:
                            rate_limit(rate_limiter)
                            process_item(source_table, destination_table, source_batch, destination_batch, item)
                            items += 1
                            last_message = log_progress(segments, items, last_message)
                
                        if 'LastEvaluatedKey' in response:
                            next_tokens[segment] = response['LastEvaluatedKey']
                        else:
                            del next_tokens[segment]
          
    duration = int(time.monotonic() - started_at)       
    print(f"Finished. Processed {items} items in segments {segments} in {duration} seconds.")
    return ({ 'segments' : segments, 'duration' : duration, 'processed' : items })
  