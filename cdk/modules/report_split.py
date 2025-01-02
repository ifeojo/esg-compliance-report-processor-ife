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
    print(missing_sections)

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


def get_section_pages(bucket_name,local_report_path):
    yaml_file = 'compliance_config.yaml'
    local_yaml_path = os.path.basename(f"tmp/{yaml_file}")
    s3_client.download_file(bucket_name,yaml_file,local_yaml_path)
    with open(local_yaml_path) as f:
        section_config = yaml.safe_load(f)
        
    processed_config = identify_pages_from_config(local_report_path, section_config)
    config_keys = list(processed_config.keys())
    
    for i in range(len(processed_config)):
        if i == len(processed_config) - 1:
            break
        
        current_section = processed_config[config_keys[i]]
        next_section = processed_config[config_keys[i+1]]
        
        start_page = min(current_section["identified_pages"])
        end_page = max(next_section["identified_pages"])
        current_section["page_ranges"] = list(range(start_page, end_page))
        
    section_pages = {section[0]: section[1]["page_ranges"] for section in processed_config.items() if "page_ranges" in section[1]}
        
    return section_pages

        
def upload_supplier_pdf(local_report_path: str, supplier_pages: List[int], bucket: str, uid: str):
    local_supplier_details_path = "/tmp/supplier_details.pdf"
    remote_supplier_details_key = "tmp/supplier_details_" + uid + ".pdf"
    
    logger.info(f"insering supplier pages into new document: {supplier_pages}")
    with pymupdf.open(local_report_path) as doc:
        new_doc = pymupdf.open()
        for page_number in supplier_pages:
            new_doc.insert_pdf(doc, from_page=page_number, to_page=page_number)
            
        new_doc.save(local_supplier_details_path)
        s3_client.upload_file(local_supplier_details_path, bucket, remote_supplier_details_key)
        new_doc.close()
        
        supplier_uri = f"s3://{bucket}/{remote_supplier_details_key}"
        
    return supplier_uri
        
    
def upload_section3_pdf(local_report_path: str, section3_pages: List[int], bucket: str, uid: str):
    local_nc_path = "/tmp/section3.pdf"
    remote_nc_key = "tmp/section3_" + uid + ".pdf"
    
    logger.info(f"insering section 3 pages into new document: {section3_pages}")
    with pymupdf.open(local_report_path) as doc:
        new_doc = pymupdf.open()
        for page_number in section3_pages:
            new_doc.insert_pdf(doc, from_page=page_number, to_page=page_number)
        new_doc.save(local_nc_path)
        s3_client.upload_file(local_nc_path, bucket, remote_nc_key)
        new_doc.close()
        nc_uri = f"s3://{bucket}/{remote_nc_key}"
        
    return nc_uri

def upload_section_pdf(local_report_path: str, section:str, section_pages: List[int], bucket: str, uid: str):
    local_nc_path = f"/tmp/{section}.pdf"
    remote_nc_key = f"tmp/{section}_" + uid + ".pdf"
    
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

def split_report(bucket: str, key: str):
    uid = "_".join(str(uuid4()).split("-"))
    local_report_path = download_report(bucket, key)
    supplier_pages = get_supplier_pages(local_report_path)
    section_pages = get_section_pages(bucket,local_report_path)
    
    valid_sections = [
        "section0a","section0b","section1","section2","section3","section4",
        "section5","section6","section7","section8","section8a","section9",
        "section10a","section10b2","section10b4","section10c"]
    
    nc_uri_dict = {}
    
    for section in valid_sections:
        if section in section_pages:
            nc_uri = upload_section_pdf(local_report_path, section, section_pages[section], bucket, uid)
            nc_uri[section] = nc_uri
        
        elif section not in section_pages:
            logger.info(f"{section} not found in section pages object\nsection pages: {section_pages}\nsummary_pages: {supplier_pages}")
            raise ValueError(f"Unable to locate {section} in report")
    
    supplier_uri = upload_supplier_pdf(local_report_path, supplier_pages, bucket, uid)

    return supplier_uri, nc_uri_dict
    