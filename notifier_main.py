import base64
import json
from httplib2 import Http

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    message_json = json.loads(pubsub_message)
    status = message_json['status']
    source = message_json['source']['repoSource']

    url = '<WEBHOOK_URL>'
    
    text = 'Hello from a Cloud Build Notifier!\nBuild of ' + str(source) + '\n' + status 
    bot_message = {'text' : text }
    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
    http_obj = Http()

    response = http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=json.dumps(bot_message),
    )

    print(response)