import math
import sys
import boto3
import json
import time

## Google authentication flow
import googleapiclient.discovery
import google_auth_oauthlib.flow
import google.oauth2.credentials

## Gmail build
import base64
import email
from apiclient import errors
from apiclient.discovery import build


## Get the timestamp from the queue, which will act as the queue name to read the ids

def timestamp_mod(ts):
  return ts.replace(":","-").replace(".","-").replace("+","-")

sqs = boto3.client('sqs')
s3 = boto3.client('s3')

## Open SQS and grab the queue name (which is the modded timestamp)
print 'getting timestamp'
QueueUrlTimestamp = "https://sqs.us-west-2.amazonaws.com/985724320380/email_ids_to_download"
timestamp_message = sqs.receive_message(QueueUrl=QueueUrlTimestamp,MaxNumberOfMessages=1,WaitTimeSeconds=20)
timestamp = timestamp_message['Messages'][0]['Body']
QueueUrlIds = "https://sqs.us-west-2.amazonaws.com/985724320380/" + timestamp_mod(timestamp)

def build_creds(timestamp):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table('tokens')
  dict_of_creds =  table.get_item(Key={"timestamp":timestamp})['Item']
  del dict_of_creds['timestamp']
  creds = google_auth_oauthlib.flow.credentials = google.oauth2.credentials.Credentials(**dict_of_creds)
  return creds

## Build resources for reading emails
print 'building credentials'
creds = build_creds(timestamp)
    
## Now that you've got the ids - go get the messages
print 'accessing gmail'
service = build('gmail', 'v1',credentials=creds)
email_address = service.users().getProfile(userId='me').execute()['emailAddress']

## Open up the queue with the ids
print 'reading queue of ids'
messages = sqs.receive_message(QueueUrl=QueueUrlIds,MaxNumberOfMessages=10,WaitTimeSeconds=20)
print 'starting id reads'
def process_id_list(id_list,email_address):
  send = []
  ## read the gmails (which are an API call to Gmail)
  for id_dict in json.loads(id_list['Body']):
#    print email_id
    msg_id = (id_dict['id'])
    email = service.users().messages().get(userId='me', id=msg_id).execute()
  
    email['email_address'] = email_address
    send.append(email)
  return send

def send_to_s3(email_data,timestamp):
    print "s3 connection"
    obj = "\n".join([json.dumps(e) for e in email_data])
    key ="e{}.{}".format(timestamp_mod(timestamp),time.time())
    s3.put_object(Body=obj,Bucket='email-data-full',Key=key)
   

## Open up the messages, which are lists of 50 ids
email_data = []
count = 0
for m in messages['Messages']:
  count += 1
  ## Buffer up the emails received
  email_data.extend(process_id_list(m,email_address))
  ## After each batch of messages, delete the message and sleep as needed
  sqs.delete_message(QueueUrl=QueueUrlIds  ,ReceiptHandle=m['ReceiptHandle'])
  ## After each 10 batches of messages, send buffer to S3
  if count % 10 == 0:
    send_to_s3(email_data,timestamp)
    email_data = []

if len(email_data)>0:
  send_to_s3(email_data,timestamp)

  
sqs.delete_queue(QueueUrl=QueueUrlIds)
sqs.delete_message(QueueUrl=QueueUrlTimestamp,ReceiptHandle=timestamp_message['Messages'][0]['ReceiptHandle'])
print "deleted timestamp from queue and id-queue" 
