import base64
import json
from datetime import datetime
from time import sleep
from google.cloud.notebooks_v1.types import schedule
from google.cloud.notebooks_v1.types import service
from google.cloud.notebooks_v1.services.notebook_service import NotebookServiceClient

def nb_schedule(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
         sample: {"name": <pipeline name>, "notebook_path": <gcs nb path>, "outputNotebookFolder": <gcs path>}
    """
    file = event
    print(f"Processing file: {file['name']}.")
    client = NotebookServiceClient()
    TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
    bucket = 'gs://' + event['bucket']
    objectkey = event['name']
    pipeline_name = 'fraud_detection_pipeline'
    notebook_path = bucket + '/' + objectkey
    outputNotebookFolder = 'gs://<>' + '/pipeline'
    name = 'projects/<>/locations/us-central1/schedules/' + pipeline_name + '_' + TIMESTAMP
    displayName = pipeline_name + '_' + TIMESTAMP

    containerImageUri = 'gcr.io/deeplearning-platform-release/base-cu113:latest'

    ExecutionTemplate = {
         "scale_tier": 'CUSTOM',
         "master_type": 'n1-standard-4',
         "input_notebook_file": notebook_path,
         "container_image_uri": containerImageUri,
         "output_notebook_folder": outputNotebookFolder,
         "job_type": 'VERTEX_AI',
         "service_account": 'notebook@<>.iam.gserviceaccount.com'
     }

    sch = {'cron_schedule': "00 18 * * 5", 'time_zone': 'Asia/Hong_Kong', 'execution_template': ExecutionTemplate}
    request = {'parent': 'projects/<>/locations/us-central1', 'schedule_id': displayName, 'schedule': sch}

    client.create_schedule(request)



