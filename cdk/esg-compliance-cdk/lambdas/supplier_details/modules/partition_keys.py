from datetime import datetime
import json
import re

import boto3

def format_company_name(company_name: str) -> str:
    name = re.sub(r'[^a-zA-Z0-9]', ' ', company_name)
    return re.sub(r' +', ' ', name)
    
def format_audit_date(date_string: str) -> str:
    bedrock_runtime = boto3.client('bedrock-runtime')
    
    prompt = "Tell me what the following date is in ISO 8601 format (YYYY-MM-DD).  You should only respond with the date."
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "system": prompt,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": date_string}
                ]
            }
        ],
        "temperature": 0.5,
        "max_tokens": 10,
        "top_k": 100,
        "top_p": 0.1
    })
    
    response = bedrock_runtime.invoke_model(
        body=body,
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
        accept="application/json",
        contentType="application/json",
    )
    
    date_response = json.loads(response["body"].read())['content'][0]['text']
    
    try:
        datetime.fromisoformat(date_response)
        return date_response
    except ValueError:
        return date_string