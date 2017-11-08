import math
import sys
import boto3
import json

sqs = boto3.client('sqs')
QueueUrl = "https://sqs.us-west-2.amazonaws.com/985724320380/email_ids_to_download"

message = sqs.receive_message(QueueUrl=QueueUrl,MaxNumberOfMessages=1,WaitTimeSeconds=20)

timestamp = message['Messages'][0]['Body']
sqs.delete_message(QueueUrl=QueueUrl,ReceiptHandle=message['Messages'][0]['ReceiptHandle'])
print "deleted"

s3 = boto3.client('s3')
obj =  s3.get_object(Bucket='email-id-lists',Key=timestamp)
ids = json.loads(obj['Body'].read())
for i in ids:
  print i
print len(ids)
