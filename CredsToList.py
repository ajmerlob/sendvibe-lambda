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
    self.ecs = boto3.client('ecs')
  
  def per_record(self, event, context):
#      ## Debug
#      if not isinstance(event, dict) or u'timestamp' not in event:
#          logging.error(type(event))
#          logging.error("timestamp not in event")
#          logging.error(event)
#          return  "not dict or missing timestamp"

      ## Configure event to be a credentials dictionary  
      for key in event:
          if 'S' in event[key]:
              event[key] = event[key]['S']
          if 'NULL' in event[key]:
              event[key] = None
  
      ## Grab the timestamp, and delete it from the credentials dict
      timestamp = event[u'timestamp']
      del event[u'timestamp']
  
      ## Load up the Credentials
      creds = google_auth_oauthlib.flow.credentials = google.oauth2.credentials.Credentials(**event)
      gmail = googleapiclient.discovery.build(
        self.API_SERVICE_NAME, self.API_VERSION, credentials=creds)
     
      ## Shoot the timestamp into the big-picture queue
      response = self.sqs.send_message(QueueUrl="https://us-west-2.queue.amazonaws.com/985724320380/email_ids_to_download",MessageBody=timestamp)

      ## Build the timestamp-titled queue to send id_lists and the ecs to extract the data from that queue
      queue = self.sqs.create_queue(QueueName=self.timestamp_mod(timestamp))
      queueName = queue['QueueUrl']
      self.ecs.run_task(taskDefinition="get-emails-test-2:1")

      ## Begin to grab the email_ids
      response = gmail.users().messages().list(userId='me').execute()

      if 'messages' in response:
          self.sqs.send_message(QueueUrl=queueName,MessageBody=json.dumps(response['messages']))
  
      logging.error("Checking nextPageTokens")
      while 'nextPageToken' in response:
          logging.error("Executing nextPageToken")
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

#      if not u'Records' in event:
#          logging.error("No 'Records' field")
#          logging.error(event)
#          return "No 'Records' field"
#      else:
#          for record in event[u'Records']:
#              if u'dynamodb' not in record or u'NewImage' not in record[u'dynamodb']:
#                  logging.error("Missing dynamodb or NewImage")
#                  logging.error(record)
#              else:
#                  try:
#                      per_record(record[u'dynamodb'][u'NewImage'],context)
#                  except Exception, e:
#                      logging.error("Some Error Occurred")
#                      logging.error(record)
#                      logging.error(event)
#  		      logging.error(e)
#          return "Completed all records"


def handler(event,context):
  c = CredsToList()
  try:
    return c.process_event(event,context)
  except Exception, e:
    logging.error(e)
    return "failed"
