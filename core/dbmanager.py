import os, sys, shutil, pickle
from collections import Counter
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
import core.utils
from core.schema import DataType, TableSchema, IndexType, SelectSchema
from indices.bplustree import BPlusTree
import logger

class DBManager:
    def __init__(self):
        self.tables_path = f"{os.path.dirname(__file__)}/../tables"
        self.logger = logger.CustomLogger("DBManager")

    def error(self, error : str):
        raise RuntimeError(error)

    def create_table(self, table_schema : TableSchema):
        path = f"{self.tables_path}/{table_schema.table_name}"
        if os.path.exists(path):
            self.error("table already exists")
        else:
            os.makedirs(path)

            if not table_schema.columns:
                self.error("the table must have at least 1 column")

            counter = Counter(column.name for column in table_schema.columns)

            repeats = [name for name, count in counter.items() if count > 1]
            if len(repeats) > 0:
                self.error(f"the table can't have multiple columns with the same name (repeated names: {",".join(repeats)})")

            with open(f"{path}/metadata.dat", "wb") as file:
                pickle.dump(table_schema, file)
            for column in table_schema.columns:
                index_type = column.index_type

                match index_type:
                    case IndexType.AVL:
                        pass
                        # AVL(table_schema, column)
                    case IndexType.ISAM:
                        pass
                        # ISAM(table_schema, column)
                    case IndexType.HASH:
                        pass
                        # HASH(table_schema, column)
                    case IndexType.BTREE:
                        BPlusTree(table_schema, column)
                    case IndexType.RTREE:
                        pass
                        # RTREE(table_schema, column)
                    case IndexType.SEQ:
                        pass
                        # SEQ(table_schema, column)
                    case _:
                        self.error("invalid index type")
            self.logger.info("Table created successfully")

    
    def get_table_schema(self, table_name : str) -> TableSchema:
        path = f"{self.tables_path}/{table_name}"
        if os.path.exists(path):
            with open(f"{path}/metadata.dat", "rb") as file:
                return pickle.load(file)
        else:
            self.error("table doesn't exist")

    def drop_table(self, table_name : str):
        path = f"{self.tables_path}/{table_name}"
        if os.path.exists(path):
            shutil.rmtree(path)
        else:
            self.error("table doesn't exist")

    def select(self, select_schema : SelectSchema):
        table = self.get_table_schema(select_schema.table_name)
        if not select_schema.all:
            column_names = [column.name for column in table.columns]
            nonexistent = [column for column in select_schema.column_list if column not in column_names]
            if nonexistent:
                self.error(f"some columns don't exist (nonexistent columns: {",".join(nonexistent)})")
        
        result = self.select_condition(table, select_schema.condition)
        return result

    def select_condition(self, table_schema : TableSchema, condition):
        pass

    def insert(self):
        pass

    def delete(self):
        pass

    def create_index(self):
        pass

    def drop_index(self):
        pass

def test():
    dbmanager = DBManager()
    import schemabuilder
    builder = schemabuilder.TableSchemaBuilder()
    builder.set_name("productos")
    builder.add_column("id", DataType.INT, True, IndexType.BTREE).add_column("nombre", DataType.VARCHAR, False, IndexType.HASH, 20)
    schema = builder.get()
    dbmanager.drop_table("productos")
    dbmanager.create_table(schema)
    read_schema = dbmanager.get_table_schema("productos")
    print(read_schema.table_name)
    for i in read_schema.columns:
        print(f"{i.data_type}, {i.index_type}, {i.is_primary}, {i.name}")


if __name__ == "__main__":
    test()
