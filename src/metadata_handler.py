import os
import json

class MetadataHandler:
    def __init__():
        pass
    
    @staticmethod
    def create_table_metadata():
        table_metadata = {
            'table_name': 'undifined',
            'table_columns': [],
            'table_num_blocks': 0
        }
        
        return table_metadata

    @staticmethod
    def read_table_metadata(table_name:str):
        metadata_path = os.path.join("data","metadata", table_name + ".json")

        with open(metadata_path, "r") as f:
            data = json.load(f)

        return data

    @staticmethod
    def write_table_metadata(table_name:str, metadata:dict):
        
        metadata_path = os.path.join("data","metadata", table_name + ".json")
        # create the metadata file
        with open(metadata_path, "w") as f:
            json.dump(metadata, f)