import json
import logging
import os

import boto3

from modules.generate_email import get_email, get_issues_markdown

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

bedrock = boto3.client('bedrock-runtime')
ddb = boto3.client('dynamodb')

supplier_table = os.environ['SUPPLIER_TABLE']

def handler(event, context):
    logger.info(f'Event: {json.dumps(event)}')
    # inspect event to see what we need to do
    base_url = os.environ['BASE_URL']
    logger.info(f"routing to api gateway: {base_url}")
    
    company_name = event["company_name"]
    audit_date = event["audit_date"]
    dashboard_link = "placeholder dashboard link" # pass in event
    
    if event['state_name'] == 'SendApprovalRequest':
        issues = get_issues_markdown(table_name=supplier_table, company_name=company_name, audit_date=audit_date)
        
        token = event['token']
        if isinstance(token, tuple):
            token = token[0]
        logger.info(f"token: {token}")
        # callback_token = unescape(token).replace(' ', '+')
        approve_url = f"{base_url}/approve?token={token}"
        reject_url = f"{base_url}/reject?token={token}"

        # Compose email
        email_subject = 'Audit Approval Request'

        email_body = f"""Hello,
The AWS ESG job has processed the following report:

Company:           {company_name}
Date of Audit:     {audit_date}

{issues}

Please check that you are happy with the results and click on the following links to approve or reject the report.
You can view the report in the dashboard here:

{dashboard_link}




Approve:
{approve_url}



Reject:
{reject_url}
        """
    elif event['state_name'] == 'SendConfirmation':
        # Compose email
        email_subject = 'Audit Email Body'
        if event['status'] == 'APPROVED':
            email_body = get_email(table_name=supplier_table, company_name=company_name, audit_date=audit_date)
        elif event['status'] == 'REJECTED':
            email_body = f"""Hello,
        
This is a notification that the following audit has been rejected upon human review.
Please raise this with the ML&AI Platform team.

Company:           {company_name}
Date of Audit:     {audit_date}"""
        else:
            raise ValueError(f"Expected status of APPROVED or REJECTED but received {event}")
        
        table_name = os.getenv("SUPPLIER_TABLE")
        ddb = boto3.resource('dynamodb')
        table = ddb.Table(table_name)
        
        response = table.get_item(Key={'Company Name': company_name, 'AuditDateIssueNumber': audit_date})
        item = response.get('Item')
        
        if item:
            status = event['status']
            response = table.update_item(
                Key={
                    'Company Name': company_name,
                    'AuditDateIssueNumber': audit_date
                },
                UpdateExpression="set approval_status = :s, email_body = :e",
                ExpressionAttributeValues={
                    ":s": status,
                    ":e": email_body
                }
            )
        
    else:
        raise ValueError(f"Expected state_name of SendConfirmation or SendApprovalRequest but received {event}")

    logger.info(f'Sending email: {email_body}')
    boto3.client('sns').publish(
        TopicArn=os.environ['TOPIC_ARN'],
        Subject=email_subject,
        Message=email_body
    )
    logger.info('done!')
    return {}