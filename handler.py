import googleapiclient.discovery
import google_auth_oauthlib.flow
import google.oauth2.credentials
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('list_ids')

def save_list(timestamp,page_token,result_list):
  item = {}
  item['timestamp'] = timestamp
  item['page_token'] = page_token
  item['ids'] = result_list
  table.put_item(Item=item)


API_SERVICE_NAME = 'gmail'
API_VERSION = 'v1'


def my_handler(event, context):
    if event is None:
        return "event was null"
    if 'timestamp' not in event:
        return "event had no timestamp"

    timestamp = event['timestamp']
    del event['timestamp']

    creds = google_auth_oauthlib.flow.credentials = google.oauth2.credentials.Credentials(**event)
    gmail = googleapiclient.discovery.build(
      API_SERVICE_NAME, API_VERSION, credentials=creds)
   
    response = gmail.users().messages().list(userId='me').execute()
    if 'nextPageToken' in response:
        save_list(timestamp,"initial",response['messages'])
    while 'nextPageToken' in response:
        page_token = response['nextPageToken']
        response = gmail.users().messages().list(userId='me', pageToken=page_token).execute()
        save_list(timestamp,page_token,response['messages']) 
    return "event success"
