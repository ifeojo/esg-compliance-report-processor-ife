import json
import boto3
import csv 
from io import StringIO
import os 
import re

table_name = os.environ["GRADINGS_TABLE"]

def standardise_text(text):
    if text is None:
        return None
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text.strip())
    # Remove spaces before and after slashes
    text = re.sub(r'\s*/\s*', '/', text)
    # Remove spaces around hyphens
    text = re.sub(r'\s*-\s*', '-', text)
    return text

def csv_to_dynamodb(s3_bucket,s3_key, table_name):
    
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    print(s3_key)
    response = s3.get_object(Bucket=s3_bucket, Key = s3_key)
    csv_content = response['Body'].read().decode('utf-8')
    csv_file = StringIO(csv_content)
    csv_reader = csv.DictReader(csv_file)
    
    for row in csv_reader:
        item = {}
        for key,value in row.items():
            
            clean_key = key.lstrip('\ufeff')

            if value == '':
                item[clean_key] = None
            
            else:
                standardized_value = standardise_text(value)
                item[clean_key] = standardized_value
        print(item)
        table.put_item(Item=item)
# print(f"Data from  has been uploaded to {table_name} table.")

def handler(event, context):
    # TODO implement
    
    print(event)
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']
    csv_to_dynamodb(s3_bucket,s3_key,table_name)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
