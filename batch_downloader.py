import math
import sys
import boto3
import json
import googleapiclient.discovery
import google_auth_oauthlib.flow
import google.oauth2.credentials
import time

## Get the S3 Key so you can get the ids

sqs = boto3.client('sqs')
QueueUrl = "https://sqs.us-west-2.amazonaws.com/985724320380/email_ids_to_download"

message = sqs.receive_message(QueueUrl=QueueUrl,MaxNumberOfMessages=1,WaitTimeSeconds=20)

timestamp = message['Messages'][0]['Body']
sqs.delete_message(QueueUrl=QueueUrl,ReceiptHandle=message['Messages'][0]['ReceiptHandle'])
print "grabbed and deleted message"

## Trade the S3 key for the messages

s3 = boto3.client('s3')
obj =  s3.get_object(Bucket='email-id-lists',Key=timestamp)
email_ids_list = json.loads(obj['Body'].read())

## Now go build the credentials stored in dynamodb
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('tokens')

dict_of_creds =  table.get_item(Key={"timestamp":timestamp})['Item']
del dict_of_creds['timestamp']
#print dict_of_creds
creds = google_auth_oauthlib.flow.credentials = google.oauth2.credentials.Credentials(**dict_of_creds)

## Now that you've got the ids - go get the messages
import base64
import email
from apiclient import errors
from apiclient.discovery import build

service = build('gmail', 'v1',credentials=creds)
email_address = service.users().getProfile(userId='me').execute()['emailAddress']

count = 0

send = []
starting_msg_id = 'initial'
print 'starting initial email mining'
for email_id in email_ids_list:
  count += 1
  msg_id = (email_id['id'])
  message = service.users().messages().get(userId='me', id=msg_id).execute()

  message['email_address'] = email_address
  send.append(message)

  ## Now that you have the message, store it back in dynamodb
  if count % 50 == 0:
    time.sleep(1)
  if count % 500 == 0:
    print email_id, "boom!"
    s3.put_object(Body=json.dumps(send),Bucket='email-data-full',Key="{}-{}-{}".format(timestamp,starting_msg_id,msg_id))
    starting_msg_id = msg_id    
    send = []