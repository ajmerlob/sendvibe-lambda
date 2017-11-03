import googleapiclient.discovery
import google_auth_oauthlib.flow
import google.oauth2.credentials
import boto3
import logging

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('list_ids')

def save_list(timestamp,page_token,result_list):
  item = {}
  logging.error("starting save")
  item['timestamp'] = timestamp
  item['page_token'] = page_token
  item['ids'] = result_list
  table.put_item(Item=item)
  logging.error("ending save")


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
    response = gmail.users().messages().list(userId='me').execute()
    save_list(timestamp,"initial",response['messages'])

    logging.error("Checking nextPageTokens")
    while 'nextPageToken' in response:
        page_token = response['nextPageToken']
        response = gmail.users().messages().list(userId='me', pageToken=page_token).execute()
        save_list(timestamp,page_token,response['messages']) 
    logging.info("record success")
    return "success"

def my_handler(event,context):
    if event is None:
        logging.error("event was null")
        return "event was null"

    if u'Records' in event:
        for record in event[u'Records']:
            if u'dynamodb' in record and u'NewImage' in record[u'dynamodb']:
                per_record(record[u'dynamodb'][u'NewImage'],context)
            else:
                logging.error("Missing dynamodb or NewImage")
                logging.error(record)
        return "got through 'em all"
    else:
        logging.error("No 'Records' field")
        logging.error(event)
        return "No 'Records' field" 
