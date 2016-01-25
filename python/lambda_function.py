# Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from __future__ import print_function

from aws_kpl_deagg.deaggregator import deaggregate_records, iter_deaggregate_records
import base64

def lambda_bulk_handler(event, context):
    
    raw_kinesis_records = event['Records']
    
    #Deaggregate all records in one call
    user_records = deaggregate_records(raw_kinesis_records)
    
    #Iterate through deaggregated records
    for record in user_records:        
        
        # Kinesis data in Python Lambdas is base64 encoded
        payload = base64.b64decode(record['kinesis']['data'])
        print('%s' % (payload))
    
    return 'Successfully processed {} records.'.format(len(user_records))

def lambda_generator_handler(event, context):
    
    raw_kinesis_records = event['Records']
    record_count = 0
    
    #Deaggregate all records using a generator function
    for record in iter_deaggregate_records(raw_kinesis_records):   
             
        # Kinesis data in Python Lambdas is base64 encoded
        payload = base64.b64decode(record['kinesis']['data'])
        print('%s' % (payload))
        record_count += 1
        
    return 'Successfully processed {} records.'.format(record_count)
