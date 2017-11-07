import googleapiclient.discovery
import google_auth_oauthlib.flow
import google.oauth2.credentials
import boto3
import logging
import time
import json

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('list_ids')

s3 = boto3.client('s3')

def save_list(timestamp,page_token,result_list):
  item = {}
  logging.error("starting save")
  item['timestamp'] = timestamp
  logging.error("ending save")
  s3.put_object(Body=json.dumps(result_list),Bucket='email-id-lists',Key=timestamp)
#  table.put_item(Item=item)
  logging.error("ending save2")

API_SERVICE_NAME = 'gmail'
API_VERSION = 'v1'


def per_record(event, context):
    if event is None:
        logging.error("event was null")
        return "event was null"
    
  
    if not isinstance(event, dict) or u'timestamp' not in event:
        logging.error(type(event))
        logging.error("timestamp not in event")
        logging.error(event)
        return  "not dict or missing timestamp"

    if event['token'] == 'ya29.Glv4BGZz0Cg1xK79AnuO2GoGii4Ig5HhK5XgVvIbbMf3mj4IHFjiKPC4Fw8ac-Lh32wxqPdtHDQcIPW1z335CB9nKempvTJaHu5275p790mS4nkhstQwPxqnvb8D':
        logging.error("somehow it was BGZz0C")
        return "was BGZz0C"

    logging.error("Appears to be working")
    logging.error(event)

    for key in event:
        if 'S' in event[key]:
            event[key] = event[key]['S']
        if 'NULL' in event[key]:
            event[key] = None

    logging.error("Still appears to be working")
    logging.error(event)

    timestamp = event[u'timestamp']
    del event[u'timestamp']

    logging.error("Still appears to be working x2")
    logging.error(event)

    creds = google_auth_oauthlib.flow.credentials = google.oauth2.credentials.Credentials(**event)
    gmail = googleapiclient.discovery.build(
      API_SERVICE_NAME, API_VERSION, credentials=creds)
   
    logging.error("Credentials Built")
    logging.error(creds.token)
    response = gmail.users().messages().list(userId='me').execute()
    messages = []
    if 'messages' in response:
        messages.extend(response['messages'])
#    save_list(timestamp,"initial",response['messages'])

    logging.error("Checking nextPageTokens")
    while 'nextPageToken' in response:
        logging.error("Executing nextPageToken")
        page_token = response['nextPageToken']
        response = gmail.users().messages().list(userId='me', pageToken=page_token).execute()
        messages.extend(response['messages'])
#        save_list(timestamp,page_token,response['messages']) 

    save_list(timestamp,None,messages)
    logging.error("record success")
    return "success"

def my_handler(event,context):
    if event is None:
        logging.error("event was null")
        return "event was null"

    if u'Records' in event:
        for record in event[u'Records']:
            if u'dynamodb' in record and u'NewImage' in record[u'dynamodb']:
                try:
                    per_record(record[u'dynamodb'][u'NewImage'],context)
                except:
                    logging.error("Some Error Occurred")
                    logging.error(record)
                    logging.error(event)
            else:
                logging.error("Missing dynamodb or NewImage")
                logging.error(record)
        return "Completed all records"
    else:
        logging.error("No 'Records' field")
        logging.error(event)
        return "No 'Records' field" 
