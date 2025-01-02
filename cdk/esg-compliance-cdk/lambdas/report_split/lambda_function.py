import json

import logging

from modules.report_split import split_report

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

def handler(event, context):
    logger.info(f"request: {json.dumps(event)}")
    
    logger.info("Getting bucket and key from event")
    
    bucket = event["detail"]["detail"]["bucket"]["name"]
    key = event["detail"]["detail"]["object"]["key"]
    import_uid = key.split('/')[0]
    
    logger.info("Splitting report")
    supplier_uri, nc_uri_list = split_report(
        bucket=bucket,
        key=key,
        import_uid=import_uid
    )

    logger.info("Returning response")
    return {
        "shortened_URIs": {
            "bucket": bucket,
            "import_uid": import_uid,
            "supplier_uri": supplier_uri,
            "nc_uri_list": nc_uri_list
        }
    }