import os

from src.column_class import Column

from src.metadata_handler import MetadataHandler
from src.utils.path_helpers import PathHelper

class SQLExecuter:
    def __init__(self):
        pass

    def create_table(self, table_name:str, columns: list[Column]):
        table_path = PathHelper.table_path(table_name)

        # check that the table doesn't exist before creating
        if not os.path.isdir(table_path):
            os.mkdir(table_path)

            # create the metadata file
            table_metadata = MetadataHandler.create_table_metadata()
            table_metadata['table_name'] = table_name
            table_metadata['table_columns'] = [i.to_dict() for i in columns]
            MetadataHandler.write_table_metadata(table_name, table_metadata)

    def create_block(self, table_name:str, block_num:int):
        table_path = PathHelper.table_path(table_name)
        block_path = PathHelper.block_path(table_name, block_num)
        
        # update number of blocks
        table_metadata = MetadataHandler.read_table_metadata(table_name)
        
        table_header = ",".join([i['column_name'] for i in table_metadata['table_columns']])

        if os.path.isdir(table_path):
            if not os.path.isfile(block_path):
                with open(block_path, "w") as b:
                    b.write(table_header + "\n")
                
                table_metadata['table_num_blocks'] += 1
                MetadataHandler.write_table_metadata(table_name, table_metadata)

    def delete_table(self, table_name:str):
        table_path = PathHelper.table_path(table_name)
        table_metadata_path = PathHelper.table_metadata_path(table_name)

        if os.path.isdir(table_path) and os.path.isfile(table_metadata_path):
            dir_walk = list(os.walk(table_path))

            # remove all the files in the table directory
            for dir in dir_walk:
                for file in dir[2]:
                    if os.path.isfile(os.path.join(dir[0], file)):
                        os.remove(os.path.join(dir[0], file))

            # remove all the folders in the table directory
            for dir in reversed(dir_walk):
                if os.path.isdir(dir[0]):
                    os.rmdir(dir[0])

            # remove the metadata of the file
            os.remove(table_metadata_path)     

    def _format_values_insert(self, columns_table:list[str], 
                              columns_insert:list[str], values_insert:list):
        
        counter_insert_column = 0
        result = []
        
        for col in columns_table:
            if col in columns_insert:
                result.append(str(values_insert[counter_insert_column]))
                counter_insert_column += 1
            else:
                result.append('')
        return result

    def insert(self, table_name:str, columns:list[str], values:list):
        table_metadata = MetadataHandler.read_table_metadata(table_name)

        columns_table = [i['column_name'] for i in table_metadata['table_columns']]
        values_to_insert = self._format_values_insert(columns_table, columns, values)
        
        block_path = PathHelper.block_path(table_name, table_metadata['table_num_blocks'] - 1)
        with open(block_path, 'a') as f:
            f.write(",".join(values_to_insert) + "\n")

if __name__ == '__main__':

    col_1 = Column('str', "col_str")
    col_2 = Column('int', "col_int")

    SQLExecuter().create_table("table1", columns=[col_1, col_2])
    SQLExecuter().create_block("table1", 0)

    SQLExecuter().insert("table1", ["col_str", "col_int"], ['A', 2])
    SQLExecuter().insert("table1", ["col_str"], ['B'])
    SQLExecuter().insert("table1", ["col_int"], [3])

    # SQLExecuter().delete_table("table1")