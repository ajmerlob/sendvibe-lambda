import googleapiclient.discovery
import google_auth_oauthlib.flow
import google.oauth2.credentials
import boto3

API_SERVICE_NAME = 'gmail'
API_VERSION = 'v1'


def my_handler(event, context):
    try:
        del event['timestamp']
    except:
        pass
    creds = google_auth_oauthlib.flow.credentials = google.oauth2.credentials.Credentials(**event)
    gmail = googleapiclient.discovery.build(
      API_SERVICE_NAME, API_VERSION, credentials=creds)
   
    response = gmail.users().messages().list(userId='me').execute()
    messages = []
    if 'nextPageToken' in response:
        messages.extend(response['messages'])
    while 'nextPageToken' in response:
        page_token = response['nextPageToken']
        response = gmail.users().messages().list(userId='me', pageToken=page_token).execute()
        messages.extend(response['messages'])

 
    return messages
