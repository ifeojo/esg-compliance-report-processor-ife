import logging
from typing import List
from uuid import uuid4

import boto3
import pymupdf
import yaml
import os 

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

    
def download_report(bucket: str, key: str) -> str:
    local_report_path = "/tmp/report.pdf"
    s3_client.download_file(bucket, key, local_report_path)
    
    return local_report_path


def identify_pages_from_config(local_path_to_doc, config) -> dict:
    for page in config.values():
        page["identified_pages"] = []
        
    with pymupdf.open(local_path_to_doc) as doc:
        for page in doc:
            text = page.get_text("text").lower()
            for obj in config.values():
                if all(term.lower() in text for term in obj["search_terms"]):
                    obj["identified_pages"].append(page.number)
        
    # in cases of info being spread across pages, look at page pairs
    missing_sections = {}
    for section, val in config.items():
        if val["identified_pages"] == []:
            missing_sections[section] = val

    with pymupdf.open(local_path_to_doc) as doc:
        for i in range(len(doc)):
            current_page = doc[i].get_text("text").lower()
            if i < len(doc) - 1:
                next_page = doc[i+1].get_text("text").lower()
                text = current_page + next_page
            else:
                text = current_page
                
            for obj in missing_sections.values():
                if all(term.lower() in text for term in obj["search_terms"]):
                    obj["identified_pages"].append(doc[i].number)
    return config
    
def get_supplier_pages(local_report_path):
    with open("config/supplier_pages.yaml") as f:
        supplier_config = yaml.safe_load(f)
    
    processed_config = identify_pages_from_config(local_report_path, supplier_config)
    page_numbers = []
    for item in processed_config.values():
        page_numbers += item["identified_pages"]
    
    padding = [num+1 for num in page_numbers]
    page_numbers = set(page_numbers+padding)
    return page_numbers


def get_section_pages(bucket_name,local_report_path, import_uid):
    yaml_file = f"{import_uid}/config/compliance_config.yaml"
    local_yaml_path = "/tmp/compliance_config.yaml"
    s3_client.download_file(bucket_name,yaml_file,local_yaml_path)
    with open(local_yaml_path) as f:
        section_config = yaml.safe_load(f)
        
    processed_config = identify_pages_from_config(local_report_path, section_config)
    print(processed_config)
    config_values = list(processed_config.values())
    config_keys = list(processed_config.keys())
    selected_clauses = [ ]
    selected_sections =[]
    for i in range(len(processed_config)):
        print(i)
        print(config_values[i].keys())
        if i == len(processed_config)-1:
            break
        if 'selected' in config_values[i].keys():
            clause = config_values[i]['clause']
            selected_section = config_keys[i]
            selected_sections.append(selected_section)
            selected_clauses.append(clause)
            
            current_section = processed_config[config_keys[i]]
            next_section = processed_config[config_keys[i+1]]
            
            start_page = min(current_section["identified_pages"])
            end_page = max(next_section["identified_pages"])
            
            current_section["page_ranges"] = list(range(start_page, end_page))
            
    section_pages = {section[0]: section[1]["page_ranges"] for section in processed_config.items() if "page_ranges" in section[1]}          
        
    return selected_sections, section_pages, selected_clauses

        
def upload_supplier_pdf(local_report_path: str, supplier_pages: List[int], bucket: str, uid: str):
    local_supplier_details_path = "/tmp/supplier_details.pdf"
    remote_supplier_details_key = f"{uid}/processing/supplier_details.pdf"
    
    logger.info(f"inserting supplier pages into new document: {supplier_pages}")
    with pymupdf.open(local_report_path) as doc:
        new_doc = pymupdf.open()
        for page_number in supplier_pages:
            new_doc.insert_pdf(doc, from_page=page_number, to_page=page_number)
            
        new_doc.save(local_supplier_details_path)
        s3_client.upload_file(local_supplier_details_path, bucket, remote_supplier_details_key)
        new_doc.close()
        
        supplier_uri = f"s3://{bucket}/{remote_supplier_details_key}"
        
    return supplier_uri
        

def upload_section_pdf(local_report_path: str, section:str, section_pages: List[int], bucket: str, uid: str):
    local_nc_path = f"/tmp/{section}.pdf"
    remote_nc_key = f"{uid}/processing/{section}_nc.pdf"
    
    logger.info(f"insering {section} pages into new document: {section_pages}")
    with pymupdf.open(local_report_path) as doc:
        new_doc = pymupdf.open()
        for page_number in section_pages:
            new_doc.insert_pdf(doc, from_page=page_number, to_page=page_number)
        new_doc.save(local_nc_path)
        s3_client.upload_file(local_nc_path, bucket, remote_nc_key)
        new_doc.close()
        nc_uri = f"s3://{bucket}/{remote_nc_key}"
        
    return nc_uri

def split_report(bucket: str, key: str, import_uid: str):
    local_report_path = download_report(bucket, key)
    supplier_pages = get_supplier_pages(local_report_path)
    sections, section_pages, clauses= get_section_pages(bucket,local_report_path,import_uid)
    supplier_uri = upload_supplier_pdf(local_report_path, supplier_pages, bucket, import_uid)
    nc_uri_list = [ ]
    for x in sections:
        
        if x not in section_pages.keys():
            logger.info(f"{x} missing from section pages object\nsection pages: {section_pages}")
            raise ValueError(f"Unable to locate {x} in report")
            
        current_clause = clauses[sections.index(x)]
        current_section_pages = section_pages[x]
        nc_uri = upload_section_pdf(local_report_path, x, current_section_pages, bucket, import_uid)
        print(nc_uri)
        dict_entry = {
            "section": x,
            "clause": current_clause,
            "nc_uri":nc_uri
        }
        nc_uri_list.append(dict_entry)

    return supplier_uri, nc_uri_list
    