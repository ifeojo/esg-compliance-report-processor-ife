import boto3 
from boto3.dynamodb.conditions import Key, Attr
import os

# Create a DynamoDB client
ddb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

supplier_table = os.environ['SUPPLIER_TABLE']



def get_issue_titles_for_clause(supplier_table,company_name, audit_date, clause):

    # Query parameters
    response = ddb.Table(supplier_table).query(
        KeyConditionExpression=Key('Company Name').eq(company_name) & Key('AuditDateIssueNumber').begins_with(audit_date),
        FilterExpression=Attr('Clause').eq(clause),
    )

    items = response['Items']

    # Handle pagination if there are more results
    while 'LastEvaluatedKey' in response:
        response = supplier_table.query(
            KeyConditionExpression=Key('Company Name').eq(company_name) & Key('AuditDateIssueNumber').begins_with(audit_date),
            FilterExpression=Attr('Clause').eq(clause),
            ProjectionExpression='Issue Title, ESG Rating, Report Timescale, Issue Type',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])

    return items


def handler(event, context):
    
    nc_uri = event["nc_uri"]
    section = event["section"]
    company_name = event["company_name"]
    audit_date = event["audit_date"]
    clause = event["clause"]
    

    bucket = nc_uri.split('/')[2]
    import_uid = nc_uri.split('/')[3]
    
    data = get_issue_titles_for_clause(supplier_table,company_name, audit_date, clause)
    
    # Convert the data to a string
    data_str = str(data)
    file_key = f"{import_uid}/processing/{section}_nc_data.txt"
    
    # Upload the string data to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=file_key,
        Body=data_str
    )
    
    return {
        'statusCode': 200,
        'body': f'Data successfully uploaded to S3: s3://{bucket}/{file_key}'
    }

