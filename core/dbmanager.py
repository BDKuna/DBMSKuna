import os, sys, shutil, pickle
from collections import Counter
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
import core.utils
from core.schema import DataType, TableSchema, IndexType, SelectSchema, Column
from indices.bplustree import BPlusTree
from indices.avltree import AVLTree
from core.record_file import Record, RecordFile
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
            # verificar que haya exactamente un primary key, si no tirar error
            # si no se indica, que se un hash

            repeats = [name for name, count in counter.items() if count > 1]
            if len(repeats) > 0:
                self.error(f"the table can't have multiple columns with the same name (repeated names: {','.join(repeats)})")

            with open(f"{path}/metadata.dat", "wb") as file:
                pickle.dump(table_schema, file)
            for column in table_schema.columns:
                index_type = column.index_type
                if index_type is None:
                    continue
                match index_type:
                    case IndexType.AVL:
                        AVLTree(table_schema, column)
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
            self.logger.error("table doesn't exists")
            #self.error("table doesn't exist")

    def select(self, select_schema : SelectSchema):
        table = self.get_table_schema(select_schema.table_name)
        if not select_schema.all:
            column_names = [column.name for column in table.columns]
            nonexistent = [column for column in select_schema.column_list if column not in column_names]
            if nonexistent:
                self.error(f"some columns don't exist (nonexistent columns: {','.join(nonexistent)})")
        
        result = self.select_condition(table, select_schema.condition)
        return result

    def select_condition(self, table_schema : TableSchema, condition):
        pass

    def insert(self, table_name:str, values: list):
        tableSchema: TableSchema = self.get_table_schema(table_name)
        if len(values) != len(tableSchema.columns):
            raise Exception("El número de valores no coincide con el número de columnas")
        record = Record(tableSchema, values)
        record_file = RecordFile(tableSchema)
        pos = record_file.append(record)

        indexes = tableSchema.get_indexes()

        #insertar los indices
        for i, index in enumerate(indexes.keys()):
            if indexes[index] is not None:
                indexes[index].insert(pos, record.values[i])

    def selectAll(self, table_name:str):
        tableSchema: TableSchema = self.get_table_schema(table_name)
        primaryIndex = tableSchema.get_primary_index()
        record_file = RecordFile(tableSchema)
        return [record_file.read(pos) for pos in primaryIndex.getAll()]
    
    def delete(self):
        pass

    def create_index(self):
        #trae schema, cambia index y guarda schema
        pass

    def drop_index(self):
        #trae schema, elimina index y guarda schema
        pass


def test():
    dbmanager = DBManager()
    import schemabuilder
    builder = schemabuilder.TableSchemaBuilder()
    builder.set_name("productos")
    builder.add_column(name="id", data_type=DataType.INT, is_primary_key=False)
    builder.add_column(name="nombre", data_type=DataType.VARCHAR, is_primary_key=True, index_type=IndexType.BTREE, varchar_length=20)
    schema:TableSchema = builder.get()
    dbmanager.drop_table("productos")
    dbmanager.create_table(schema)
    read_schema = dbmanager.get_table_schema("productos")

    dbmanager.insert(read_schema.table_name, [4, "Eduardo"])
    dbmanager.insert(read_schema.table_name, [5, "Paca"])
    dbmanager.insert(read_schema.table_name, [6, "Sergod"])
    dbmanager.insert(read_schema.table_name, [9, "Sergod2"])
    dbmanager.insert(read_schema.table_name, [5, "Sergod3"])
    dbmanager.insert(read_schema.table_name, [4, "Sergod4"])
    dbmanager.insert(read_schema.table_name, [2, "Sergod5"])
    dbmanager.insert(read_schema.table_name, [7, "Sergod6"])
    dbmanager.insert(read_schema.table_name, [1, "Buenas tardes"])
    dbmanager.insert(read_schema.table_name, [10, "Hola"])
    indexes = schema.get_indexes()
    for index in indexes.keys():
        if indexes[index] is not None:
            print(indexes[index])

    for i in dbmanager.selectAll(schema.table_name):
        print(i)


if __name__ == "__main__":
    test()
