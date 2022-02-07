from typing import Dict
from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
import base64
import json
from httplib2 import Http

def predict_tabular_classification_sample(
    project: str,
    endpoint_id: str,
    instance_dict: Dict,
    location: str = "us-central1",
    api_endpoint: str = "us-central1-aiplatform.googleapis.com",
):
    # The AI Platform services require regional API endpoints.
    client_options = {"api_endpoint": api_endpoint}
    # Initialize client that will be used to create and send requests.
    # This client only needs to be created once, and can be reused for multiple requests.
    client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
    # for more info on the instance schema, please use get_model_sample.py
    # and look at the yaml found in instance_schema_uri
    instance = json_format.ParseDict(instance_dict, Value())
    instances = [instance]
    parameters_dict = {}
    parameters = json_format.ParseDict(parameters_dict, Value())
    endpoint = client.endpoint_path(
        project=project, location=location, endpoint=endpoint_id
    )
    response = client.predict(
        endpoint=endpoint, instances=instances, parameters=parameters
    )
    print("response")
    print(" deployed_model_id:", response.deployed_model_id)
    # See gs://google-cloud-aiplatform/schema/predict/prediction/tabular_classification_1.0.0.yaml for the format of the predictions.
    predictions = response.predictions
    #for prediction in predictions:
    #    print(" prediction:", dict(prediction))
    return predictions



def predict(event, context):
    
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    message_json = json.loads(pubsub_message)
    message_dict = { "card_transactions_amount": float(message_json['tx_id']), 
                    "card_transactions_transaction_distance": float(message_json['card_transactions_transaction_distance']),
                    "card_transactions_transaction_hour_of_day": str(message_json['card_transactions_transaction_hour_of_day']), 
                    "category": str(message_json["category"])}
    tx_id = message_json['tx_id']
    
    predictions = predict_tabular_classification_sample(
        project="<>",
        endpoint_id="<>",
        location="us-central1",
        instance_dict=message_dict
    )
    for prediction in predictions:
        print(" prediction:", dict(prediction))
        tmp = dict(prediction)
    
    if tmp['scores'][0] < tmp['scores'][1]:
        text = "Fraudulent Transaction Detected: " + str(tx_id) + " \nscore = " + str(tmp['scores'][1])
    else:
        text = "Transaction Inspected: " + str(tx_id) + " \nscore = " + str(tmp['scores'][1])
        
    url = '<WEBHOOK_URL>'
    
    # text = 'Hello from a Cloud Build Notifier!\nBuild of ' + str(source) + '\n' + status 
    bot_message = {'text' : text }
    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
    http_obj = Http()

    response = http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=json.dumps(bot_message),
    )
        
