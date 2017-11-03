import googleapiclient.discovery
import google_auth_oauthlib.flow
import google.oauth2.credentials

API_SERVICE_NAME = 'gmail'
API_VERSION = 'v1'


def my_handler(event, context):
    del event['timestamp']
    creds = google_auth_oauthlib.flow.credentials = google.oauth2.credentials.Credentials(**event)
    gmail = googleapiclient.discovery.build(
      API_SERVICE_NAME, API_VERSION, credentials=creds)
   
    results = gmail.users().messages().list(userId='me').execute()
 
    return results
