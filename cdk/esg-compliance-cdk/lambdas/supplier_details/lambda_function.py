import json
import os

import logging

from modules.supplier_extraction import get_supplier_details
from modules.dynamo_upload import create_audit_record

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

def handler(event, context):
    logger.info(f"request: {json.dumps(event)}")
    
    supplier_uri = event["supplier_uri"]
    
    logger.info(f"Getting supplier details from:\n {supplier_uri}")
    supplier_details = get_supplier_details(supplier_uri)
    
    table_name = os.getenv("SUPPLIER_TABLE")
    logger.info(f"Using dynamodb table: {table_name}")
    logger.info(f"Uploading supplier details to dynamodb:\n {supplier_details}")
    response = create_audit_record(supplier_dict=supplier_details, table_name=table_name)
    logger.info(f"Dynamodb response:\n {response}")
    
    return {
        "supplier_uri": event["supplier_uri"],
        "nc_uri_list": event["nc_uri_list"],
        "company_name": supplier_details['Company Name'],
        "audit_date":supplier_details['Date of Audit'],
    }