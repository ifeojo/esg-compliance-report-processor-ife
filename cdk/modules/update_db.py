import boto3

def update_approved_item(table_name: str, company_name: str, audit_date: str, email_body: str):
    ddb = boto3.resource('dynamodb')
    table = ddb.Table(table_name)
    
    response = table.UpdateItem(
        Key = {
            'Company Name': company_name,
            'AuditDateIssueNumber': audit_date
        },
        UpdateExpression="set #s = :s, #e = :e",
        ExpressionAttributeNames={
            '#s': 'ApprovalStatus',
            '#e': 'EmailBody'
        },
        ExpressionAttributeValues={
            ':s': 'APPROVED',
            ':e':email_body
        }
    )
    return response

def update_rejected_item(table_name: str, company_name: str, audit_date: str):
    ddb = boto3.resource('dynamodb') 
    table = ddb.Table(table_name)

    response = table.UpdateItem(
        Key = {
            'Company Name': company_name,
            'AuditDateIssueNumber': audit_date
        },
        UpdateExpression="set #s = :s",
        ExpressionAttributeNames={
            '#s': 'ApprovalStatus'
        },
        ExpressionAttributeValues={
            ':s': 'REJECTED'
        }
    )
    return response