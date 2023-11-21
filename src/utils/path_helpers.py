import os

class PathHelper:
    def __init__(self):
        pass
    
    @staticmethod
    def table_metadata_path(table_name:str):
        return os.path.join("data", "metadata", table_name + ".json")

    @staticmethod
    def table_path(table_name:str):
        return os.path.join(os.path.join("data", "files", table_name))
    
    @staticmethod
    def block_path(table_name:str, block_num:int):
        return os.path.join("data", "files", table_name, 
                                  table_name + "_" + str(block_num) + ".csv")