import os
import json

class MetadataHandler:
    def __init__():
        pass
    
    @staticmethod
    def read_table_metadata(table_name:str):
        pass

    @staticmethod
    def write_table_metadata(table_name:str, metadata:dict):
        
        metadata_path = os.path.join("data","metadata", table_name + ".json")
        # create the metadata file
        with open(metadata_path, "w") as f:
            json.dump(metadata, f)