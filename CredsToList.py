import googleapiclient.discovery
import google_auth_oauthlib.flow
import google.oauth2.credentials
import boto3
import logging
import time
import json

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

class CredsToList:
  API_SERVICE_NAME = 'gmail'
  API_VERSION = 'v1'
  
  def timestamp_mod(self,ts):
    return ts.replace(":","-").replace(".","-").replace("+","-")

  def __init__(self):
    self.sqs = boto3.client('sqs')
    self.batch = boto3.client('batch')
  
  def per_record(self, event, context):
      ## Configure event to be a credentials dictionary  
      for key in event:
          if 'S' in event[key]:
              event[key] = event[key]['S']
          if 'NULL' in event[key]:
              event[key] = None
  
      ## Grab the timestamp and key, and delete it from the credentials dict
      logging.error(event)
      timestamp = event[u'timestamp']
      email_address = event[u'key']
      del event[u'timestamp']
      del event[u'key']
  
      ## Load up the Credentials
      creds = google_auth_oauthlib.flow.credentials = google.oauth2.credentials.Credentials(**event)
      gmail = googleapiclient.discovery.build(
        self.API_SERVICE_NAME, self.API_VERSION, credentials=creds)
     
      ## Shoot the timestamp into the big-picture queue
      response = self.sqs.send_message(QueueUrl="https://us-west-2.queue.amazonaws.com/985724320380/email_ids_to_download",MessageBody=timestamp)

      ## Build the timestamp-titled queue to send id_lists and the batch to extract the data from that queue
      queue = self.sqs.create_queue(QueueName=self.timestamp_mod(timestamp))
      queueName = queue['QueueUrl']
      jobName = "t{}".format(self.timestamp_mod(timestamp))
      self.batch.submit_job(jobName=jobName,jobQueue='first-run-job-queue',jobDefinition="attempt3:2")

      ## Begin to grab the email_ids
      response = gmail.users().messages().list(userId='me').execute()

      ## Send the first batch of email_ids to SQS
      if 'messages' in response:
          self.sqs.send_message(QueueUrl=queueName,MessageBody=json.dumps(response['messages']))
  
      ## Send the remaining batches of email_ids to SQS
      while 'nextPageToken' in response:
          page_token = response['nextPageToken']
          response = gmail.users().messages().list(userId='me', pageToken=page_token).execute()
          self.sqs.send_message(QueueUrl=queueName,MessageBody=json.dumps(response['messages']))
  
      logging.error("record success")
      return "success"
  
  def process_event(self, event,context):
      if event is None:
          logging.error("event was null")
          return "event was null"
  
      for record in event[u'Records']:
          self.per_record(record[u'dynamodb'][u'NewImage'],context)
      return "records completed"

def handler(event,context):
  c = CredsToList()
#  try:
  return c.process_event(event,context)
#  except Exception, e:
#    logging.error(e)
#    return "failed"
