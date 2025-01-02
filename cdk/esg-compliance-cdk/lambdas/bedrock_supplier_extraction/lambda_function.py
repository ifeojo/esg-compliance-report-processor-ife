import json
import logging
import os

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

from modules.bedrock_extraction import supplier_extract
from modules.dynamo_upload import create_audit_record

def handler(event, context):
    logger.info(f"request: {json.dumps(event)}")
    
    supplier_uri = event["supplier_uri"]
    
    logger.info(f"Getting supplier details from:\n {supplier_uri}")
    supplier_details = supplier_extract(supplier_uri)
    
    table_name = os.getenv("SUPPLIER_TABLE")
    logger.info(f"Using dynamodb table: {table_name}")
    logger.info(f"Uploading supplier details to dynamodb:\n {supplier_details}")
    response = create_audit_record(supplier_dict=supplier_details, table_name=table_name)
    logger.info(f"Dynamodb response:\n {response}")
    
    return {
        "supplier_uri": event["supplier_uri"],
        "nc_uri_list": event["nc_uri_list"],
        "company_name": supplier_details['Company Name'],
        "audit_date":supplier_details['Date Of Audit'],
    }