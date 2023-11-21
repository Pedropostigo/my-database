import os

from metadata_handler import MetadataHandler

class SQLExecuter:
    def __init__(self):
        pass

    def create_table(self, table_name:str):
        table_path = os.path.join("data", "files", table_name)

        # check that the table doesn't exist before creating
        if not os.path.isdir(table_path):
            os.mkdir(table_path)

            # create the metadata file
            table_metadata = MetadataHandler.create_table_metadata()
            table_metadata['table_name'] = table_name
            MetadataHandler.write_table_metadata(table_name, table_metadata)

    def create_block(self, table_name:str, block_num:int):
        table_path = os.path.join("data", "files", table_name)
        block_path = os.path.join("data", "files", table_name, 
                                  table_name + "_" + str(block_num) + ".csv")
        
        if os.path.isdir(table_path):
            if not os.path.isfile(block_path):
                with open(block_path, "w") as b:
                    b.write("Hello wold")
                
                # update number of blocks
                table_metadata = MetadataHandler.read_table_metadata(table_name)
                table_metadata['table_num_blocks'] += 1
                MetadataHandler.write_table_metadata(table_name, table_metadata)

if __name__ == '__main__':
    SQLExecuter().create_table("table1")
    SQLExecuter().create_block("table1", 0)