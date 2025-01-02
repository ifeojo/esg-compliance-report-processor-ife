from typing import Dict

import boto3

def create_audit_record(supplier_dict: Dict, table_name: str) -> Dict:
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.put_item(Item=supplier_dict)
    return response