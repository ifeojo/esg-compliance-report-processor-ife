from typing import Dict

import yaml
import pandas as pd

class AuditTable:
    def __init__(self, config: Dict):
        self.columns = [col["column"] for col in config["schema"]]
        self.data_types = [col["type"] for col in config["schema"]]
        
        self.index = pd.MultiIndex.from_tuples(self.columns)
        
    def build_table(self, table_data: pd.DataFrame) -> pd.DataFrame:
        num_levels = self.index.nlevels
        
        df = table_data.copy()
        df = df.iloc[num_levels:]
        df.reset_index(drop=True, inplace=True)
        
        df.columns = self.index
        
        # need to handle enforcing datatypes for each column
        
        df.set_index(df.columns[0], inplace=True)
        
        return df
         
        
def audit_table_factory(table_name: str):
    with open("config/tables.yaml") as f:
        config = yaml.safe_load(f)
        
    if table_name not in config:
        raise ValueError(f"Table {table_name} not found in config")
    
    return AuditTable(config[table_name])