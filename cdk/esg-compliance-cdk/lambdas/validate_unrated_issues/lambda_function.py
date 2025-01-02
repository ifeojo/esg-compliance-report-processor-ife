import os
import boto3
import faiss
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores.faiss import FAISS

br= boto3.client('bedrock')
bedrock = boto3.client('bedrock-runtime')
ddb = boto3.client('dynamodb')

compliance_grading_table = os.environ['GRADINGS_TABLE']
supplier_table = os.environ['SUPPLIER_TABLE']
      
def get_dynamo_db_data(table_name):
    response = ddb.scan(
        TableName=table_name
    )  
    items = response['Items']

    ratings_tuples = [
    (
        item.get('Issue Title',{}).get('S','no issue'),
        item.get('Updated Grading',{}).get('S','no rating'),
        item.get('Resolution Window',{}).get('S','no timeline')
    )
    for item in items
    ]
    
    return ratings_tuples
        
def generate_embeddings(ratings_tuples):
    embeddings = BedrockEmbeddings(client=bedrock,model_id='cohere.embed-english-v3')
    
    issues = [rating[0] for rating in ratings_tuples]
    ratings = [rating[1] for rating in ratings_tuples]
    timeframes = [rating[2] for rating in ratings_tuples]

    metadata = [{'issue': issue, 'rating': rating, 'timeframe': timeframe} for issue, rating, timeframe in zip(issues,ratings,timeframes)]
    vector_db = FAISS.from_texts(issues, embeddings, metadatas=metadata)
    return vector_db
        
def get_closest(vector_db, issue_title):
    query = f"Which issue title is the closest match to this: {issue_title}"
    closest_issue = vector_db.similarity_search(query,k=1)
    issue_dict = closest_issue[0].metadata
    return issue_dict

def add_issue_to_dynamodb(supplier_table, ddb_entry):
    
    response = ddb.put_item(
        TableName=supplier_table,
        Item=ddb_entry
    )
    return response
def handler(event,context):
    
    clause = event['clause']
    company_name = event['company_name']
    audit_date = event["audit_date"]
    unrated_issues = event['unrated_issues']
    
    ratings_tuples = get_dynamo_db_data(compliance_grading_table)
    vector_db = generate_embeddings(ratings_tuples)
    
    for issue in unrated_issues:
        audit_date_issue_no=issue[0]
        nc_observation = issue[1]
        issue_title=issue[2]
        explanation=issue[3]
        timescale=issue[4]
        
        issue_dict = get_closest(vector_db, issue_title)
        
        esg_remediation_timescale = issue_dict['timeframe']
        rating = issue_dict['rating']
        
        if esg_remediation_timescale.strip() == timescale.strip():
            print('match!')
                            
            ddb_entry = {
                'Company Name': {'S': company_name},
                'AuditDateIssueNumber': {'S': audit_date_issue_no},
                'Date Of Audit': {'S': audit_date},
                'Clause': {'S':clause},
                'Issue Type': {'S': nc_observation},
                'Issue Title': {'S': issue_title},
                'Report Timescale': {'S': timescale},
                'Report Explanation': {'S': explanation},
                'ESG Rating': {'S': rating},
                'ESG Timescale': {'S': esg_remediation_timescale},
                'Exact Issue Title':{'S':'No'},
                'Timescales Match': {'S': 'Yes'},
            }
        else: 
            print('no match!')
            ddb_entry = {
                'Company Name': {'S': company_name},
                'AuditDateIssueNumber': {'S': audit_date_issue_no},
                'Date Of Audit': {'S': audit_date},
                'Clause': {'S':clause},
                'Issue Type': {'S': nc_observation},
                'Issue Title': {'S': issue_title},
                'Report Timescale': {'S': timescale},
                'Report Explanation': {'S': explanation},
                'ESG Rating': {'S': rating},
                'ESG Timescale': {'S': esg_remediation_timescale},
                'Exact Issue Title':{'S':'No'},
                'Timescales Match': {'S': 'No'},
            }
        
        add_issue_to_dynamodb(supplier_table, ddb_entry)
        
    return {
        "company_name": company_name,
        "audit_date": audit_date,
    }
    
        
 
            
    
    
    
    
    