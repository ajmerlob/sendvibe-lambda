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


class Gmining:
  
  ## Get the timestamp from the queue, which will act as the queue name to read the ids
  
  def timestamp_mod(self,ts):
    return ts.replace(":","-").replace(".","-").replace("+","-")
 
  def build_creds(self):
    dynamodb = boto3.resource('dynamodb',region='us-west-2')
    table = dynamodb.Table('tokens')
    dict_of_creds =  table.get_item(Key={"timestamp":self.timestamp})['Item']
    del dict_of_creds['timestamp']
    assert u'refresh_token' in dict_of_creds and dict_of_creds['refresh_token'] != u"", "refresh token was missing or blank"
    creds = google_auth_oauthlib.flow.credentials = google.oauth2.credentials.Credentials(**dict_of_creds)
    return creds

  def refresh_auth(self):
    print 'ISSUE: refreshing auth'
    creds = self.build_creds()
    self.service = build('gmail', 'v1',credentials=creds)

  def process_id_list(self,id_list):
    send = []
    ## read the gmails (which are an API call to Gmail)
    for id_dict in json.loads(id_list['Body']):
  #    print email_id
      msg_id = (id_dict['id'])
      try:
        email = self.service.users().messages().get(userId='me', id=msg_id).execute()
      except google.auth.exceptions.RefreshError:
        self.refresh_auth()
        email = self.service.users().messages().get(userId='me', id=msg_id).execute()
        
    
      email['email_address'] = self.email_address
      send.append(email)
    return send

  def send_to_s3(self,email_data):
      print "s3 connection - writing {} emails".format(len(email_data))
      obj = "\n".join([json.dumps(e) for e in email_data])
      key ="e{}.{}".format(self.timestamp_mod(self.timestamp),time.time())
      self.s3.put_object(Body=obj,Bucket='email-data-full',Key=key)

  def __init__(self):
    self.sqs = boto3.client('sqs',region='us-west-2')
    self.s3 = boto3.client('s3',region='us-west-2')
  
    ## Open SQS and grab the queue name (which is the modded timestamp)
    print 'getting timestamp'
    self.QueueUrlTimestamp = "https://sqs.us-west-2.amazonaws.com/985724320380/email_ids_to_download"
    timestamp_message = self.sqs.receive_message(QueueUrl=self.QueueUrlTimestamp,MaxNumberOfMessages=1,WaitTimeSeconds=20)
    self.rh = timestamp_message['Messages'][0]['ReceiptHandle']
    self.sqs.delete_message(QueueUrl=self.QueueUrlTimestamp,ReceiptHandle=self.rh)
    self.timestamp = timestamp_message['Messages'][0]['Body']
    self.QueueUrlIds = "https://sqs.us-west-2.amazonaws.com/985724320380/" + self.timestamp_mod(self.timestamp)
  
    ## Build resources for reading emails
    print 'building credentials'
    creds = self.build_creds()
      
  ## Now that you've got the ids - go get the messages
    print 'accessing gmail'
    self.service = build('gmail', 'v1',credentials=creds)
    try:
      self.email_address = self.service.users().getProfile(userId='me').execute()['emailAddress']
    except google.auth.exceptions.RefreshError, e:
      print e
      raise Exception("{} was messed up".format(self.timestamp))
      
    print self.email_address
  
  def attempt_read_queue(self):
    for attempt in range(3):
      try:
        list_of_id_lists = self.sqs.receive_message(QueueUrl=self.QueueUrlIds,MaxNumberOfMessages=10,WaitTimeSeconds=20)
        assert 'Messages' in list_of_id_lists, "Queue with ids was empty or returned nothing after 20 seconds - attempt {}".format(attempt+1)
        return list_of_id_lists
      except AssertionError, ae:
        print ae
    return None
    

  def read_queue(self):
    ## Use the SQS queue with the ids
    print 'reading queue of ids'
    list_of_id_lists = self.attempt_read_queue() 
    assert list_of_id_lists is not None, "Queue deemed empty"

    print 'starting id reads'
    ## Open up the messages, which are lists of 50 ids
    email_data = []
    count = 0
    for id_list in list_of_id_lists['Messages']:
      count += 1
      ## Buffer up the emails received
      email_data.extend(self.process_id_list(id_list))
      ## After each batch of messages, delete the message and sleep as needed
      self.sqs.delete_message(QueueUrl=self.QueueUrlIds  ,ReceiptHandle=id_list['ReceiptHandle'])

    ## After all the reading, send buffer to S3
    self.send_to_s3(email_data)

  def final_clean(self):
    self.sqs.delete_queue(QueueUrl=self.QueueUrlIds)
    print "deleted id-list queue (even if it had messages in it)" 
    

g = Gmining()    
try:
  while True:
    g.read_queue()
except AssertionError, e:
  print e
finally:
  g.final_clean()
