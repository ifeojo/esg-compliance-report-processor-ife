from typing import List
from uuid import uuid4

import boto3
import pymupdf
    
def download_report(bucket: str, key: str, s3_client) -> str:
    local_report_path = "/tmp/report.pdf"
    s3_client.download_file(bucket, key, local_report_path)
    
    return local_report_path
    
def find_pages_to_split(local_report_path):
    target_pages = []

    with pymupdf.open(local_report_path) as doc:
        target = "Summary of Findings"
        for page in doc[1:]:
            text = page.get_text()
            if target in text:
                contents_page = doc[page.number]
                break
            
        links = contents_page.get_links()
        for link in links:
            _link = contents_page.get_textbox(link['from']).lower().replace('\n', ' ')
            if _link in ["0a - universal rights covering ungp"]:
                summary_start = link['page']
            elif _link in ["3 - working conditions are safe and hygienic", "4 - child labour shall not be used"]:
                target_pages.append(link['page'])
                
    # should be sorted anyway due to search order
    target_pages.sort()
    
    return summary_start, target_pages

        
def upload_supplier_pdf(local_report_path: str, summary_start: int, s3_client, bucket: str, uid: str):
    local_supplier_details_path = "/tmp/supplier_details.pdf"
    remote_supplier_details_key = "tmp/supplier_details_" + uid + ".pdf"
    
    with pymupdf.open(local_report_path) as doc:
        new_doc = pymupdf.open()
        new_doc.insert_pdf(doc, from_page=1, to_page=summary_start-1)
        new_doc.save(local_supplier_details_path)
        s3_client.upload_file(local_supplier_details_path, bucket, remote_supplier_details_key)
        supplier_uri = f"s3://{bucket}/{remote_supplier_details_key}"
        
    return supplier_uri
        
    
def upload_nc_pdf(local_report_path: str, target_pages: List[int], s3_client, bucket: str, uid: str):
    local_nc_path = "/tmp/section3.pdf"
    remote_nc_key = "tmp/section3_" + uid + ".pdf"
    
    with pymupdf.open(local_report_path) as doc:
        new_doc = pymupdf.open()
        new_doc.insert_pdf(doc, from_page = target_pages[0], to_page = target_pages[1]-1)
        new_doc.save(local_nc_path)
        s3_client.upload_file(local_nc_path, bucket, remote_nc_key)
        nc_uri = f"s3://{bucket}/{remote_nc_key}"
        
    return nc_uri

def split_report(bucket: str, key: str):
    s3_client = boto3.client("s3")
    uid = "_".join(str(uuid4()).split("-"))
    
    local_report_path = download_report(bucket, key, s3_client)
    summary_start, target_pages = find_pages_to_split(local_report_path)
    
    supplier_uri = upload_supplier_pdf(local_report_path, summary_start, s3_client, bucket, uid)
    nc_uri = upload_nc_pdf(local_report_path, target_pages, s3_client, bucket, uid)

    return supplier_uri, nc_uri
    