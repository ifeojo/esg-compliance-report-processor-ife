import boto3 

# Create a DynamoDB client
ddb = boto3.client('dynamodb')
s3_client = boto3.client('s3')


def handler(event, context):
    
    nc_uri = event["nc_uri"]
    bucket = nc_uri.split('/')[2]
    import_uid = nc_uri.split('/')[3]

    status_key = f"{import_uid}/status/status.txt"

    status = "completed"
    
    # Upload the string data to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=status_key,
        Body=status
    )
    
    return {
        'statusCode': 200,
        'body': f'Status successfully uploaded'
    }

