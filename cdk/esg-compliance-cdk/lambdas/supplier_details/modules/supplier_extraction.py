from typing import Dict

import boto3
import pandas as pd

from textractcaller.t_call import call_textract, Textract_Features
from textractor import Textractor
from textractor.data.constants import TextractFeatures

from modules import tables, partition_keys


def get_supplier_form_details(supplier_uri: str) -> Dict:
    extractor = Textractor()
    
    document = extractor.start_document_analysis(
        file_source=supplier_uri,
        features=[TextractFeatures.FORMS],
        save_image=False
    )
    
    form_keys = [
        "Site Name",
        "Company Name",
        "Site contact and job title",
        "Site e-mail",
        "Site phone",
        "GPS Address",
        "Coordinates",
        "Date of Audit",
        "Audit type",
        "Audit Company Name",
        "Announced type",
    ]

    factory_details = {}
    for key in form_keys:
        key_value = document.get(key)
        if key_value == []:
            continue
        try:
            _, value = str(key_value)[1:-1].split(" : ")
        except Exception as e:
            print(f"failed to split {key_value.get_text()}")
            raise e
        factory_details[key] = value
        
    # enforce ISO 8601 for the date
    factory_details["Date of Audit"] = partition_keys.format_audit_date(factory_details["Date of Audit"]) 
    factory_details["Company Name"] = partition_keys.format_company_name(factory_details["Company Name"])
    factory_details["AuditDateIssueNumber"] = factory_details["Date of Audit"]
    
    return factory_details

def get_supplier_table_details(supplier_uri: str):
    extractor = Textractor()

    document = extractor.start_document_analysis(
        file_source=supplier_uri,
        features=[TextractFeatures.TABLES],
        save_image=False
    )
    
    tables_of_interest = {}
    # Currently neglects audit attendance table due to lack of table title in the audit
    for table in document.tables:
        if table is None or table.title is None:
            continue
        title = table.title.text.lower()
        if title in ["summary of findings", "worker analysis"]:
            tables_of_interest[title] = table
        
    worker_analysis_df = tables_of_interest["worker analysis"].to_pandas()
    worker_analysis = tables.audit_table_factory("Worker analysis")
    workers_table = worker_analysis.build_table(table_data=worker_analysis_df)
    
    summary_of_findings_df = tables_of_interest["summary of findings"].to_pandas()
    summary_of_findings = tables.audit_table_factory("Summary of findings")
    summary_table = summary_of_findings.build_table(table_data=summary_of_findings_df)
    
    tables_combined = {
        "workers_table": workers_table.to_json(orient="split"),
        "summary_table": summary_table.to_json(orient="split"),
    }
    
    return tables_combined

def get_supplier_details(supplier_uri: str) -> Dict:
    form_data = get_supplier_form_details(supplier_uri=supplier_uri)
    table_data = get_supplier_table_details(supplier_uri=supplier_uri)
    
    return {**form_data, **table_data}
    
    