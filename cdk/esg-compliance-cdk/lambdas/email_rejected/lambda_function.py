import json
import logging
import os

import boto3
from html import unescape

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

def handler(event, context):
    logger.info(event)
    
    sfn_client = boto3.client('stepfunctions')
    
    task_token = event['queryStringParameters']['token']
    task_token = unescape(task_token).replace(' ', '+')
    
    if event['path'] == '/reject' and event['httpMethod'] == 'GET':
        sfn_client.send_task_success(
            taskToken=task_token,
            output=json.dumps({
                'status': 'REJECTED'
            })
        )
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': 'Approval successful'
    }