import streamlit as st
import pandas as pd
import numpy as np
import boto3
import time
from streamlit_pdf_viewer import pdf_viewer
import yaml
import uuid
import streamlit as st
from st_files_connection import FilesConnection
import s3fs
import base64
import random
import json
import ast
import os
from streamlit_cognito_auth import CognitoHostedUIAuthenticator
from streamlit_cognito_auth.session_provider import Boto3SessionProvider
from botocore.exceptions import ClientError

pool_id = os.environ["COGNITO_USER_POOL_ID"]
app_client_id = os.environ["COGNITO_APP_CLIENT_ID"]
app_client_secret = os.environ["COGNITO_APP_CLIENT_SECRET"]
cognito_domain = os.environ["COGNITO_DOMAIN"]
redirect_uri = os.environ["COGNITO_REDIRECT_URI"]
region = os.environ["AWS_REGION"]
identity_pool_id = os.environ["COGNITO_IDENTITY_POOL_ID"]
aws_account_id = os.environ["AWS_ACCOUNT_ID"]

report_bucket = os.environ["REPORT_BUCKET"]
grading_bucket = os.environ["GRADING_BUCKET"]


authenticator = CognitoHostedUIAuthenticator(
    pool_id=pool_id,
    app_client_id=app_client_id,
    app_client_secret=app_client_secret,
    cognito_domain=cognito_domain,
    redirect_uri=redirect_uri,
    use_cookies=False
)

session_provider = Boto3SessionProvider(
    region=region,
    account_id=aws_account_id,
    user_pool_id=pool_id,
    identity_pool_id=identity_pool_id,
)

s3_client = boto3.client('s3')
s3_uid = str(uuid.uuid4())

def sleep_interval():
    return 10

def get_authenticated_status():
    is_logged_in = authenticator.login()
    return is_logged_in
def logout():
    authenticator.cookie_manager.reset_credentials()
    authenticator.logout()
    st.stop()

def read_s3_content(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return content
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            st.error(f"The file {key} does not exist in the bucket {bucket}.")
        elif error_code == 'AccessDenied':
            st.error(f"Access denied to the file {key} in bucket {bucket}. Check your permissions.")
        else:
            st.error(f"An error occurred: {e}")
        return None
    
def create_yaml_config(end_search_term, compliance_clauses, options, s3_prefix, local_yaml_path, remote_yaml_key):
        checkboxes = {}
        
        #looping through selected options
        for option in options:
            current_selected = 'yes'
            current_section_name = f"section{option.split('-')[0].lower().strip()}"
            if len(option.split('-')) == 2:
                current_search_term = " ".join(option.split(' ')[2:]).lower()
            else: 
                current_search_term = " ".join(option.split(' ')[:-1])
            next_index = compliance_clauses.index(option) + 1 
            next_option = compliance_clauses[next_index] if next_index < len(compliance_clauses) else None
            
            if next_option:
                next_section_name = f"section{next_option.split('-')[0].lower().strip()}"
                if len(next_option.split('-')) == 2:
                    next_search_term = " ".join(next_option.split(' ')[2:]).lower()
                else:
                    next_search_term = " ".join(next_option.split(' ')[:-1])
                    
                checkboxes[option] = {
                    "section":current_section_name,
                    "search_terms":[
                        current_search_term,
                        end_search_term
                        ],
                    "next_section":next_section_name,
                    "next_search_terms":[
                        next_search_term,
                        end_search_term
                    ]}
            else:
                checkboxes[option] = {
                    "section":current_section_name,
                    "search_terms":[
                        current_search_term,
                        end_search_term
                    ]}


        transformed_data = {}
        
        for key, value in checkboxes.items():
            if "section" in value.keys():
                section_key = value["section"]
                transformed_data[section_key] = {"search_terms": value["search_terms"],
                                                "selected": 'yes',
                                                "clause": key
                                                }
            if "next_section" in value.keys():
                next_section_key = value["next_section"]
                transformed_data[next_section_key] = {"search_terms": value["next_search_terms"]}
        
        yaml_content = yaml.dump(transformed_data, default_flow_style=False)
        
        # Write the YAML content to a file
        with open(local_yaml_path, 'w') as file:
            file.write(yaml_content)
        # yaml_file_dest = f"{s3_prefix}/{config}/{remote_yaml_key}"

        s3_client.upload_file(local_yaml_path, report_bucket, remote_yaml_key)

def upload_report_to_s3(file, bucket,s3_file):
        
        try:
            s3_client.upload_fileobj(file,bucket,s3_file)
            st.success('File Succesfully Uploaded')
            return True 
        except FileNotFoundError: 
            # Sleep to prevent excessive API calls while polling for status
            time.sleep(sleep_interval())
            st.error('File not found.')
            return False 

def check_processing_status(s3_uid):
    data_key = f"{s3_uid}/status/status.txt"
    try:
        response = s3_client.get_object(Bucket=report_bucket, Key=data_key)
        return 'completed'
    except s3_client.exceptions.NoSuchKey:
        return 'running'
    except Exception as e:
        st.error(f"An error occurred while checking status: {str(e)}")
        return 'error'
        
def check_file_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        return False


is_logged_in = get_authenticated_status()

if is_logged_in:
    st.session_state['authenticated'] = True

if not is_logged_in:
    st.stop()
    
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

if 'keywords' not in st.session_state:
    st.session_state.keywords = []
    
if st.session_state['authenticated']:
    credentials = authenticator.get_credentials()

    session_provider.setup_default_session(credentials.id_token, region_name=region)
        

    end_search_term = "current systems and evidence examined"
    compliance_clauses =  [
            "0A - Universal rights covering UNGP",
            "0B - Management systems and code implementation",
            "1 - Freely chosen employment",
            "2 - Freedom of association and right to collective bargaining are respected",
            "3 - Working conditions are safe and hygienic",
            "4 - Child labour shall not be used",
            "5 - Living wages are paid",
            "6 - Working hours are not excessive",
            "7 - No discrimination is practiced",
            "8 - Regular employment is provided",
            "8A - No harsh or inhumane treatment is allowed",
            "9 - No harsh or inhumane treatment is allowed",
            "10A - Entitlement to work and immigration",
            "10B2 - Environment 2-pillar",
            "10B4 - Environment 4-pillar",
            "10C - Business ethics 4-pillar"]

    

    
        
    st.title('ESG Compliance Extraction')

    with st.form('Gradings'):
        uploaded_gradings = st.file_uploader("Upload Compliance Gradings", type=("csv"))
        
        if uploaded_gradings:
            if uploaded_gradings.type != "text/csv":
                st.error(f'{uploaded_gradings.type} not supported')

            else:
                st.success(uploaded_gradings.name + ' Selected')
                
        upload_botton = st.form_submit_button('Upload')
        
    if upload_botton:
        gradings_name = uploaded_gradings.name
        local_gradings_path = "compliance_gradings.csv"
        s3_dest = f"gradings/{s3_uid}.csv"
        
        with st.spinner('Uploading...'):
            upload_report_to_s3(uploaded_gradings,grading_bucket,s3_dest)

    with st.form('Non Compliance Clauses'):

        
        uploaded_report = st.file_uploader("Upload an ESG Report", type=("pdf"))

        if uploaded_report is not None:
            if uploaded_report.type != "application/pdf":
                st.error(f'{uploaded_report.type} not supported')
                
            else:
                st.success(uploaded_report.name + ' Selected')
        
        options = st.multiselect(
            'What non-compliance clause(s) would you like to extract?',
            compliance_clauses,
            )
        
        extract_button = st.form_submit_button('Extract')

    if extract_button:
        report_name = uploaded_report.name
        local_yaml_path = "compliance_config.yaml"
        remote_yaml_key = f"{s3_uid}/config/compliance_config.yaml"
        s3_dest = f"{s3_uid}/inputs/{report_name}"
        
        create_yaml_config(end_search_term, compliance_clauses, options, s3_uid, local_yaml_path, remote_yaml_key)
        
        with st.spinner('Uploading...'):
            upload_report_to_s3(uploaded_report,report_bucket,s3_dest)
            
        if options: 
            
            with st.spinner('Reading report...Come back in a minute or two'):
                status = 'running'
                while status == 'running':
                    status = check_processing_status(s3_uid)
                    if status == 'running':
                        # Sleep to prevent excessive API calls while polling for status
                        time.sleep(sleep_interval())  #Wait for 10 seconds before checking again 
            
            if status == 'completed':
                    st.success('Processing completed successfully!')
            else:
                st.error('Processing failed.')
            
            tabs = st.tabs(options)
            
            for i, tab in enumerate(tabs):
                
                with tab:   
                    current_section_name = f"section{options[i].split('-')[0].lower().strip()}"
                    data_key = f"{s3_uid}/processing/{current_section_name}_nc_data.txt"
                    # try:
                    response = s3_client.get_object(Bucket=report_bucket, Key=data_key)
                    content = response["Body"].read().decode("utf-8")
                    data = ast.literal_eval(content)
                    
                    df = pd.DataFrame(data)
                    
                    if df.empty:
                        st.warning('No data found for this section.')
                    else: 
                        st.dataframe(
                        df,
                        column_config={
                            "name": "Company Name",
                            "date": "Audit Date",
                        },
                        hide_index=True,
                        )
                    
                    with st.expander("Do you want to verify with the report?"):
                        shortened_pdf_key = f"{s3_uid}/processing/{current_section_name}_nc.pdf"
                        
                        if check_file_exists(report_bucket, shortened_pdf_key):
                            st.success('File exists')
                            local_pdf_path = f"{current_section_name}_{i}.pdf"    
                            s3_client.download_file(report_bucket, shortened_pdf_key, local_pdf_path)
                            with open(local_pdf_path,"rb") as f:
                                base64_pdf = base64.b64encode(f.read()).decode('utf-8')
                        
                            pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="800" height="800" type="application/pdf"></iframe>'
                            st.markdown(pdf_display, unsafe_allow_html=True)
                        else: 
                            st.write('PDF not available yet...')
                    
                    with st.expander("Do you want to view the email?"):
                        shortened_email_key = f"{s3_uid}/email/email.txt"
                        
                        if check_file_exists(report_bucket, shortened_email_key):
                            st.success('File exists')
                            email_content = read_s3_content(report_bucket, shortened_email_key)

                            if email_content:
                                st.write(email_content)
                            else:
                                st.write('Email not available or could not be read.')
                        else: 
                            st.write('Email not available yet...')


            

    






